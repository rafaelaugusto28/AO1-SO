/*
 * =============================================================================
 *  INF0336 - Primeira Avaliacao (A01)
 *  Emulando o Sistema de Funcionamento do PIX
 *  Equipe: <Lucas Almeida Oliveira Isaac>, <Lucas Boclin Cunha Borges>, <Rafael Augusto Dias Batista>
 * =============================================================================
 *
 *  Compilar:  gcc -Wall -O2 -pthread -o pix.out pix.c
 *
 *  Executar:
 *    QUESTAO 1   ./pix.out q1 N T S          N envios (N:1), pool de T threads,
 *                                            S = 0 (SEM sincronismo) | 1 (COM mutex)
 *                ./pix.out experimento T R   varia N, repete R vezes cada N
 *                                            (a partir de que N o problema aparece)
 *    QUESTAO 2   ./pix.out q2 C              C = 1 
 *                                            C = 2 mesmo exemplo na ordem P2, P3, P1
 *                                            C = 3 caso PIX: 8 envios, pool de 3 threads
 *    QUESTAO 3   ./pix.out q3 N T CAP        Ctrl+C mostra o estado da fila
 *                                            Ctrl+\ encerra o programa
 *
 *  Modelo N:1 -> N usuarios (contas 1..N) enviam um PIX de R$ 1,00 para a
 *  MESMA conta recebedora (conta 0). Resultado correto: saldo da conta 0 = R$ N,00
 *  e o total de dinheiro no sistema nao muda.
 *
 *  Organizacao do arquivo:
 *    PARTE 0 - Base comum: contas, requisicoes, relogio
 *    PARTE 1 - QUESTAO 1: thread pool + base de contas compartilhada
 *                         (condicao de corrida x mutex) + experimento de N
 *    PARTE 2 - QUESTAO 2: fila FCFS para as requisicoes que superam a pool
 *    PARTE 3 - QUESTAO 3: debugging por sinais
 *    main    - escolhe a questao pela linha de comando
 * =============================================================================
 */
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* =============================================================================
 *  PARTE 0 - BASE COMUM
 * ============================================================================= */
#define VALOR_PIX 100   /* R$ 1,00 em centavos (evita erro de ponto flutuante) */
#define UT_MS     100   /* Q2/Q3: 1 unidade de tempo (ut) = 100 ms              */
#define MAX_T     64    /* tamanho maximo da thread pool                        */

typedef struct {
    volatile long saldo;   /* centavos; volatile forca leitura/escrita na memoria */
    pthread_mutex_t lock;  /* uma trava por conta (solucao da Q1)                 */
} conta_t;

typedef struct {
    int id;                      /* numero do envio (P1, P2...); -1 = encerra a thread */
    int origem, destino;
    long valor;
    int chegada_ut, burst_ut;    /* Q2/Q3: instante de chegada e tempo de execucao */
    double t_chegada, t_inicio, t_fim;   /* Q2/Q3: tempos medidos (ut)             */
    int worker;                          /* Q2/Q3: thread da pool que atendeu      */
} requisicao_t;

static conta_t *contas;          /* RECURSO COMPARTILHADO: a "base de dados" de contas */
static requisicao_t *reqs;       /* requisicoes de envio */
static int N, T;                 /* numero de envios e tamanho da pool */

/* Cria a base no modelo N:1: conta 0 = recebedor, contas 1..N com R$ 1,00 cada.
 * O envio i sai da conta i+1 e vai para a conta 0. */
static void cria_base(int n) {
    N = n;
    contas = malloc(sizeof(conta_t) * (N + 1));
    reqs   = calloc(N, sizeof(requisicao_t));
    for (int i = 0; i <= N; i++) {
        contas[i].saldo = (i == 0) ? 0 : VALOR_PIX;
        pthread_mutex_init(&contas[i].lock, NULL);
    }
    for (int i = 0; i < N; i++) {
        reqs[i].id = i + 1;
        reqs[i].origem = i + 1;
        reqs[i].destino = 0;
        reqs[i].valor = VALOR_PIX;
    }
}

static void libera_base(void) {
    for (int i = 0; i <= N; i++) pthread_mutex_destroy(&contas[i].lock);
    free(contas);
    free(reqs);
}

static long soma_saldos(void) {
    long s = 0;
    for (int i = 0; i <= N; i++) s += contas[i].saldo;
    return s;
}

static double agora_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1000.0 + t.tv_nsec / 1e6;
}

/* =============================================================================
 *  PARTE 1 - QUESTAO 1
 * -----------------------------------------------------------------------------
 *  Thread pool: T threads fixas atendem as N requisicoes. Nao
 *  se cria uma thread por envio. A thread w atende os envios w, w+T, w+2T...
 *
 *  Elementos fundamentais:
 *   - RECURSO compartilhado : vetor contas[]; no modelo N:1 todos disputam
 *                             contas[0].saldo (o saldo do recebedor)
 *   - SECAO CRITICA         : leitura-modificacao-escrita dos saldos (transferir)
 *   - CONDICAO DE CORRIDA   : duas threads leem o MESMO saldo do recebedor (ex.: 5),
 *                             cada uma soma R$ 1,00 e escreve 6 -> um credito se
 *                             perde, mas os dois pagadores foram debitados: o
 *                             dinheiro "some" do sistema
 *
 *  Solucao: um mutex POSIX por conta. As duas contas sao travadas
 *  SEMPRE na mesma ordem (menor id primeiro), o que elimina a espera circular e
 *  previne deadlock.
 *
 *  Experimento: o modo "experimento" roda varios N e conta quantas execucoes
 *  terminaram inconsistentes, mostrando a partir de que N o problema fica evidente.
 * ============================================================================= */
static int SYNC;          /* 0 = sem sincronismo, 1 = com mutex */
static sem_t largada;     /* semaforo: libera as T threads da pool ao mesmo tempo */

/* Q1 - transferencia SEM sincronismo (gera a condicao de corrida) */
static void transferir_sem_sync(requisicao_t *r) {
    /* ===== inicio da SECAO CRITICA (desprotegida) ===== */
    long saldo_origem = contas[r->origem].saldo;           /* leitura */
    if (saldo_origem < r->valor) return;                   /* saldo insuficiente */
    long saldo_destino = contas[r->destino].saldo;         /* leitura */
    contas[r->origem].saldo  = saldo_origem - r->valor;    /* escrita */
    contas[r->destino].saldo = saldo_destino + r->valor;   /* escrita */
    /* ===== fim da SECAO CRITICA ===== */
}

/* Q1 - transferencia COM mutex (tambem usada pelas Q2 e Q3) */
static void transferir_com_sync(requisicao_t *r) {
    int a = r->origem < r->destino ? r->origem : r->destino;   /* menor id */
    int b = r->origem < r->destino ? r->destino : r->origem;   /* maior id */
    pthread_mutex_lock(&contas[a].lock);                        /* acquire() */
    pthread_mutex_lock(&contas[b].lock);
    /* ===== SECAO CRITICA (protegida) ===== */
    long saldo_origem = contas[r->origem].saldo;
    if (saldo_origem >= r->valor) {
        long saldo_destino = contas[r->destino].saldo;
        contas[r->origem].saldo  = saldo_origem - r->valor;
        contas[r->destino].saldo = saldo_destino + r->valor;
    }
    /* ===== fim da SECAO CRITICA ===== */
    pthread_mutex_unlock(&contas[b].lock);                      /* release() */
    pthread_mutex_unlock(&contas[a].lock);
}

/* Q1 - thread da pool */
static void *worker_q1(void *arg) {
    int w = *(int *)arg;
    sem_wait(&largada);                       /* aguarda a largada */
    for (int i = w; i < N; i += T) {
        if (SYNC) transferir_com_sync(&reqs[i]);
        else      transferir_sem_sync(&reqs[i]);
    }
    pthread_exit(0);
}

/* Q1 - executa uma rodada; retorna o numero de creditos perdidos */
static long executa_q1(int n, int t, int sync, double *tempo_ms, long *total_antes, long *total_depois) {
    cria_base(n);
    T = t;
    SYNC = sync;
    *total_antes = soma_saldos();

    pthread_t tid[MAX_T];
    int ids[MAX_T];
    sem_init(&largada, 0, 0);
    for (int w = 0; w < T; w++) {
        ids[w] = w;
        pthread_create(&tid[w], NULL, worker_q1, &ids[w]);
    }
    double t0 = agora_ms();
    for (int w = 0; w < T; w++) sem_post(&largada);    /* todas comecam juntas */
    for (int w = 0; w < T; w++) pthread_join(tid[w], NULL);
    *tempo_ms = agora_ms() - t0;
    sem_destroy(&largada);

    *total_depois = soma_saldos();
    long perdidos = ((long)N * VALOR_PIX - contas[0].saldo) / VALOR_PIX;
    libera_base();
    return perdidos;
}

/* Q1 - uma execucao com relatorio */
static void modo_q1(int n, int t, int sync) {
    double tempo;
    long antes, depois;
    long perdidos = executa_q1(n, t, sync, &tempo, &antes, &depois);
    long esperado = (long)n * VALOR_PIX, obtido = esperado - perdidos * VALOR_PIX;
    printf("=== PIX N:1 | N=%d envios | pool=%d threads | %s ===\n",
           n, t, sync ? "COM mutex" : "SEM sincronismo");
    printf("Saldo esperado do recebedor : R$ %ld,%02ld\n", esperado / 100, esperado % 100);
    printf("Saldo obtido do recebedor   : R$ %ld,%02ld\n", obtido / 100, obtido % 100);
    printf("Creditos perdidos           : %ld\n", perdidos);
    printf("Dinheiro no sistema         : antes R$ %ld,%02ld | depois R$ %ld,%02ld\n",
           antes / 100, antes % 100, depois / 100, depois % 100);
    printf("Resultado                   : %s\n",
           (perdidos == 0 && antes == depois) ? "CONSISTENTE" : "INCONSISTENTE (condicao de corrida!)");
    printf("Tempo de processamento      : %.3f ms\n", tempo);
}

/* Q1 - experimento: a partir de que N o problema se torna evidente? */
static void modo_experimento(int t, int r) {
    int lista_n[] = {2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 50000, 100000, 1000000};
    int qtd_n = sizeof(lista_n) / sizeof(lista_n[0]);
    printf("Experimento Q1 | pool T=%d threads | %d repeticoes por N | CPUs online: %ld\n\n",
           t, r, sysconf(_SC_NPROCESSORS_ONLN));
    printf("%9s | %-26s %-12s %-10s %-10s | %-15s %-10s\n", "N", "SEM sync: inconsistentes",
           "perda media", "perda max", "tempo(ms)", "COM mutex: inc.", "tempo(ms)");
    for (int k = 0; k < qtd_n; k++) {
        int n = lista_n[k];
        int erros[2] = {0, 0};
        long soma_p = 0, max_p = 0;
        double tempo_tot[2] = {0, 0};
        for (int sync = 0; sync <= 1; sync++) {
            for (int i = 0; i < r; i++) {
                double tempo;
                long antes, depois;
                long p = executa_q1(n, t, sync, &tempo, &antes, &depois);
                tempo_tot[sync] += tempo;
                if (p != 0 || antes != depois) erros[sync]++;
                if (sync == 0) { soma_p += p; if (p > max_p) max_p = p; }
            }
        }
        printf("%9d | %4d/%-4d (%5.1f%%)          %-12.1f %-10ld %-10.2f | %4d/%-10d %-10.2f\n",
               n, erros[0], r, 100.0 * erros[0] / r, (double)soma_p / r, max_p, tempo_tot[0] / r,
               erros[1], r, tempo_tot[1] / r);
        fflush(stdout);
    }
}

/* =============================================================================
 *  PARTE 2 - QUESTAO 2
 * -----------------------------------------------------------------------------
 *  Acrescenta a Q1 uma FILA FIFO entre o servidor PIX e a thread pool:
 *    - se ha thread livre, o envio e atendido na hora;
 *    - se as T threads estao ocupadas, o envio espera na fila, e o primeiro que
 *      chegou e o primeiro atendido: FCFS.
 *
 *  E o problema do produtor-consumidor com buffer limitado:
 *    servidor PIX (produtor) -> FILA[CAP] -> pool de T threads (consumidores)
 *    semaforos: vazio = CAP, cheio = 0, mutex_fila = 1
 *
 *  Por que protege o sistema: o numero de threads fica limitado a T (nao cria
 *  uma thread por envio) e a fila e limitada (CAP). Se encher, o servidor
 *  espera em sem_wait(&vazio) em vez de acumular requisicoes sem limite.
 *  A transferencia continua protegida pelo mutex das contas (Q1).
 *
 *  Tempos simulados: cada envio tem um burst (usleep). 1 ut = 100 ms.
 *  O relatorio mostra espera, turnaround e o diagrama de Gantt.
 * ============================================================================= */
static requisicao_t **fila;              /* buffer circular */
static int CAP, ini = 0, fim = 0, qtd = 0;
static sem_t vazio, cheio, mutex_fila;   /* produtor-consumidor */
static sem_t mutex_print;                /* evita que as linhas de log se misturem */
static double t0_ms;                     /* instante zero da simulacao */

/* [Q3] estado acompanhado para o debugging (protegido por mutex_fila) */
static requisicao_t *atual[MAX_T];       /* envio em execucao em cada thread (NULL = ociosa) */
static int concluidas = 0, chegaram = 0;

static double agora_ut(void) { return (agora_ms() - t0_ms) / UT_MS; }

static void imprime_fila(void) {         /* chamar com mutex_fila travado */
    printf("fila=[");
    for (int k = 0, p = 0; k < qtd; k++) {
        requisicao_t *r = fila[(ini + k) % CAP];
        if (r->id > 0) printf("%sP%d", p++ ? " " : "", r->id);
    }
    printf("]");
}

/* Q2 - produtor: insere no FIM da fila */
static void enfileirar(requisicao_t *r) {
    sem_wait(&vazio);                    /* fila cheia -> o servidor espera */
    sem_wait(&mutex_fila);
    fila[fim] = r;
    fim = (fim + 1) % CAP;
    qtd++;
    if (r->id > 0) {
        chegaram++;                                              /* [Q3] */
        double t = agora_ut();
        sem_wait(&mutex_print);
        if (t - r->t_chegada < 0.05)     /* entrou na fila assim que chegou */
            printf("[t=%5.1f] chegada   P%-3d (burst %2d ut)   ", t, r->id, r->burst_ut);
        else                             /* teve que esperar vaga na fila cheia */
            printf("[t=%5.1f] P%-3d entra na fila (chegou em t=%.1f)   ", t, r->id, r->t_chegada);
        imprime_fila();
        printf("\n");
        sem_post(&mutex_print);
    }
    sem_post(&mutex_fila);
    sem_post(&cheio);
}

/* Q2 - consumidor: retira do INICIO da fila (FCFS) */
static requisicao_t *desenfileirar(int w) {
    sem_wait(&cheio);                    /* fila vazia -> a thread dorme na pool */
    sem_wait(&mutex_fila);
    requisicao_t *r = fila[ini];
    ini = (ini + 1) % CAP;
    qtd--;
    if (r->id > 0) {
        r->t_inicio = agora_ut();
        r->worker = w;
        atual[w] = r;                                            /* [Q3] */
    }
    sem_post(&mutex_fila);
    sem_post(&vazio);
    return r;
}

/* Q2 - thread da pool, agora alimentada pela fila FCFS */
static void *worker_q2(void *arg) {
    int w = *(int *)arg;
    while (1) {
        requisicao_t *r = desenfileirar(w);
        if (r->id < 0) break;                       /* marcador de encerramento */
        sem_wait(&mutex_print);
        printf("[t=%5.1f] thread W%d inicia  P%-3d (esperou %4.1f ut)\n",
               r->t_inicio, w, r->id, r->t_inicio - r->t_chegada);
        sem_post(&mutex_print);

        usleep(r->burst_ut * UT_MS * 1000);         /* processamento simulado (validacao, antifraude...) */
        transferir_com_sync(r);                     /* efetiva o PIX (solucao da Q1) */

        sem_wait(&mutex_fila);
        r->t_fim = agora_ut();
        atual[w] = NULL;                                         /* [Q3] */
        concluidas++;                                            /* [Q3] */
        sem_post(&mutex_fila);
        sem_wait(&mutex_print);
        printf("[t=%5.1f] thread W%d termina P%-3d\n", r->t_fim, w, r->id);
        sem_post(&mutex_print);
    }
    pthread_exit(0);
}

/* Q2 - casos de exemplo */
static void carrega_cenario_q2(int c) {
    if (c == 1 || c == 2) {              /* exemplo FCFS do slide (aula le06) */
        int b1[] = {24, 3, 3}, b2[] = {3, 3, 24};
        cria_base(3);
        T = 1; CAP = 4;
        for (int i = 0; i < 3; i++) { reqs[i].burst_ut = (c == 1) ? b1[i] : b2[i]; reqs[i].chegada_ut = 0; }
        if (c == 2) { reqs[0].id = 2; reqs[1].id = 3; reqs[2].id = 1; }   /* ordem P2, P3, P1 */
    } else {                             /* caso PIX: 8 envios, pool de 3 threads */
        int b[] = {5, 3, 8, 2, 4, 6, 1, 3};
        int a[] = {0, 0, 1, 1, 2, 3, 3, 4};
        cria_base(8);
        T = 3; CAP = 8;
        for (int i = 0; i < 8; i++) { reqs[i].burst_ut = b[i]; reqs[i].chegada_ut = a[i]; }
    }
}

/* Q2 - relatorio FCFS: tabela, medias e diagrama de Gantt */
static void relatorio_fcfs(void) {
    printf("\n%-5s %-8s %-6s %-7s %-7s %-7s %-8s %-10s\n",
           "Envio", "chegada", "burst", "thread", "inicio", "fim", "espera", "turnaround");
    double soma_espera = 0, soma_turn = 0;
    int tmax = 0;
    for (int i = 0; i < N; i++) {
        requisicao_t *r = &reqs[i];
        double espera = r->t_inicio - r->t_chegada, turn = r->t_fim - r->t_chegada;
        soma_espera += espera;
        soma_turn += turn;
        if ((int)(r->t_fim + 0.5) > tmax) tmax = (int)(r->t_fim + 0.5);
        printf("P%-4d %-8.1f %-6d W%-6d %-7.1f %-7.1f %-8.1f %-10.1f\n",
               r->id, r->t_chegada, r->burst_ut, r->worker, r->t_inicio, r->t_fim, espera, turn);
    }
    printf("Tempo medio de espera     : %.2f ut\n", soma_espera / N);
    printf("Tempo medio de turnaround : %.2f ut\n", soma_turn / N);

    printf("\nDiagrama de Gantt (3 colunas = 1 ut; '.' = thread ociosa)\nt   ");
    for (int k = 0; k <= tmax; k += 5) printf("%-15d", k);
    printf("\n");
    for (int w = 0; w < T; w++) {
        char linha[4096];
        int len = 0;
        for (int i = 0; i < N; i++) {
            if (reqs[i].worker != w) continue;
            int a = 3 * (int)(reqs[i].t_inicio + 0.5), b = 3 * (int)(reqs[i].t_fim + 0.5);
            if (b >= 4000) break;
            while (len < a) linha[len++] = '.';
            char rot[16];
            int n = snprintf(rot, sizeof rot, "|P%d", reqs[i].id);
            if (n > b - a) n = snprintf(rot, sizeof rot, "|%d", reqs[i].id);
            for (int k = 0; a + k < b; k++) linha[len++] = (k < n) ? rot[k] : '-';
        }
        linha[len] = '\0';
        printf("W%d  %s|\n", w, linha);
    }
}

/* =============================================================================
 *  PARTE 3 - QUESTAO 3
 * -----------------------------------------------------------------------------
 *  Acrescenta a Q2 um tratador de sinal definido pelo usuario:
 *    Ctrl+C            -> o terminal envia SIGINT  -> mostra o estado da fila
 *    kill -USR1 <pid>  -> SIGUSR1                  -> mostra o estado da fila
 *    Ctrl+\            -> SIGQUIT (tratador padrao) -> encerra
 *
 *  Sinais em programa multithread, opcao D: "Assign a specific
 *  thread to receive all signals for the process":
 *    1. main associa o tratador com signal(), como no signal.c;
 *    2. main bloqueia SIGINT/SIGUSR1 (pthread_sigmask) ANTES de criar as threads;
 *       elas herdam a mascara, entao o servidor e a pool nunca sao interrompidos;
 *    3. so a thread de depuracao desbloqueia esses sinais e os recebe;
 *    4. o tratador so marca uma flag; a thread de depuracao trava mutex_fila e
 *       imprime uma "fotografia" consistente da fila (printf/mutex nao sao
 *       seguros dentro do tratador, que pode interromper quem segura o mutex).
 *
 *  Tempos coerentes: chegadas a cada 0-4 ut e bursts de 5 a 20 ut (0,5 a 2 s),
 *  dando tempo de apertar Ctrl+C varias vezes e ver a fila andar.
 * ============================================================================= */
static volatile sig_atomic_t pedido_debug = 0, sinal_recebido = 0;
static volatile int terminou = 0;

/* Q3 - tratador de sinal (definido pelo usuario) */
static void tratador_debug(int sig) {
    sinal_recebido = sig;
    pedido_debug = 1;          /* so marca o pedido */
}

/* Q3 - fotografia do estado atual da fila e da pool */
static void imprime_estado(void) {
    sem_wait(&mutex_print);
    sem_wait(&mutex_fila);                       /* congela a fila durante a leitura */
    double t = agora_ut();
    printf("\n+================ DEBUG (%s) t=%.1f ut ================+\n",
           sinal_recebido == SIGINT ? "SIGINT / Ctrl+C" : "SIGUSR1", t);
    printf("| Fila FCFS (%d/%d posicoes) - ordem de atendimento:\n", qtd, CAP);
    int pos = 0, encerr = 0;
    for (int k = 0; k < qtd; k++) {
        requisicao_t *r = fila[(ini + k) % CAP];
        if (r->id < 0) { encerr++; continue; }
        printf("|   %d. P%-3d conta %-3d -> conta %d  R$ %ld,%02ld  burst %2d ut  esperando ha %5.1f ut\n",
               ++pos, r->id, r->origem, r->destino, r->valor / 100, r->valor % 100,
               r->burst_ut, t - r->t_chegada);
    }
    if (pos == 0) printf("|   (sem envios aguardando)\n");
    if (encerr) printf("|   (+%d marcador(es) de encerramento das threads)\n", encerr);
    if (qtd == CAP) printf("|   ** fila cheia: novas chegadas aguardam vaga (sem_wait(vazio)) **\n");
    printf("| Thread pool (%d threads):\n", T);
    for (int w = 0; w < T; w++) {
        if (atual[w]) printf("|   W%d: executando P%-3d (%.1f de %d ut)\n",
                             w, atual[w]->id, t - atual[w]->t_inicio, atual[w]->burst_ut);
        else          printf("|   W%d: ociosa\n", w);
    }
    pthread_mutex_lock(&contas[0].lock);
    long saldo0 = contas[0].saldo;
    pthread_mutex_unlock(&contas[0].lock);
    printf("| Concluidos: %d | Ainda fora da fila: %d | Saldo recebedor: R$ %ld,%02ld\n",
           concluidas, N - chegaram, saldo0 / 100, saldo0 % 100);
    printf("+=================================================================+\n\n");
    sem_post(&mutex_fila);
    sem_post(&mutex_print);
}

/* Q3 - thread de depuracao: a unica que recebe SIGINT/SIGUSR1 */
static void *thread_debug(void *arg) {
    (void)arg;
    sigset_t s;
    sigemptyset(&s);
    sigaddset(&s, SIGINT);
    sigaddset(&s, SIGUSR1);
    pthread_sigmask(SIG_UNBLOCK, &s, NULL);
    while (!terminou) {
        usleep(50 * 1000);
        if (pedido_debug) {
            pedido_debug = 0;
            imprime_estado();
        }
    }
    pthread_exit(0);
}

/* Q3 - carga com tempos de execucao coerentes */
static void carrega_carga_q3(int n, int t, int cap) {
    cria_base(n);
    T = t; CAP = cap;
    srand(7);
    int chegada = 0;
    for (int i = 0; i < N; i++) {
        reqs[i].chegada_ut = chegada;
        reqs[i].burst_ut = 5 + rand() % 16;     /* 5 a 20 ut */
        chegada += rand() % 5;                  /* proxima chegada em 0 a 4 ut */
    }
}

/* =============================================================================
 *  Execucao do sistema com fila (Q2) e, se pedido, com debugging (Q3)
 * ============================================================================= */
static void executa_fila(int com_debug) {
    fila = malloc(sizeof(requisicao_t *) * CAP);
    sem_init(&vazio, 0, CAP);
    sem_init(&cheio, 0, 0);
    sem_init(&mutex_fila, 0, 1);
    sem_init(&mutex_print, 0, 1);

    pthread_t tid[MAX_T], tdbg;
    int ids[MAX_T];

    if (com_debug) {                                             /* [Q3] */
        if (signal(SIGINT, tratador_debug) == SIG_ERR || signal(SIGUSR1, tratador_debug) == SIG_ERR)
            printf("Can't catch the signal :(\n");
        sigset_t s;
        sigemptyset(&s);
        sigaddset(&s, SIGINT);
        sigaddset(&s, SIGUSR1);
        pthread_sigmask(SIG_BLOCK, &s, NULL);    /* as threads criadas herdam o bloqueio */
        printf("PID %d | Ctrl+C (ou kill -USR1 %d) mostra a fila | Ctrl+\\ encerra\n", getpid(), getpid());
    }
    printf("=== PIX + FCFS | %d envios (N:1) | pool=%d threads | fila=%d | 1 ut = %d ms ===\n",
           N, T, CAP, UT_MS);

    t0_ms = agora_ms();
    if (com_debug) pthread_create(&tdbg, NULL, thread_debug, NULL);   /* [Q3] */
    for (int w = 0; w < T; w++) {
        ids[w] = w;
        pthread_create(&tid[w], NULL, worker_q2, &ids[w]);
    }

    /* servidor PIX (produtor): recebe cada envio no seu instante de chegada */
    for (int i = 0; i < N; i++) {
        double espera = reqs[i].chegada_ut - agora_ut();
        if (espera > 0) usleep((useconds_t)(espera * UT_MS * 1000));
        reqs[i].t_chegada = agora_ut();

        sem_wait(&mutex_fila);           /* so para avisar no log se a fila esta cheia */
        int cheia = (qtd == CAP);
        sem_post(&mutex_fila);
        if (cheia) {
            sem_wait(&mutex_print);
            printf("[t=%5.1f] chegada   P%-3d (burst %2d ut)   fila cheia -> aguardando vaga\n",
                   reqs[i].t_chegada, reqs[i].id, reqs[i].burst_ut);
            sem_post(&mutex_print);
        }
        enfileirar(&reqs[i]);
    }
    requisicao_t fim_req[MAX_T];                 /* um marcador de fim por thread */
    for (int w = 0; w < T; w++) { fim_req[w].id = -1; enfileirar(&fim_req[w]); }
    for (int w = 0; w < T; w++) pthread_join(tid[w], NULL);

    if (com_debug) {                                             /* [Q3] */
        terminou = 1;
        pthread_join(tdbg, NULL);
        double soma = 0;
        for (int i = 0; i < N; i++) soma += reqs[i].t_inicio - reqs[i].t_chegada;
        printf("\nFim: %d envios concluidos | espera media na fila: %.2f ut\n", concluidas, soma / N);
    } else {
        relatorio_fcfs();
    }
    printf("\nSaldo do recebedor: R$ %ld,%02ld (esperado R$ %d,00) | dinheiro conservado: %s\n",
           contas[0].saldo / 100, contas[0].saldo % 100, N,
           soma_saldos() == (long)N * VALOR_PIX ? "sim" : "NAO");

    sem_destroy(&vazio); sem_destroy(&cheio); sem_destroy(&mutex_fila); sem_destroy(&mutex_print);
    free(fila);
    libera_base();
}

/* =============================================================================
 *  main
 * ============================================================================= */
static void uso(const char *p) {
    fprintf(stderr,
            "Uso:\n"
            "  %s q1 N T S          (Q1) S=0 sem sincronismo | S=1 com mutex\n"
            "  %s experimento T R   (Q1) varia N com R repeticoes\n"
            "  %s q2 C              (Q2) C=1 slide 24,3,3 | C=2 slide 3,3,24 | C=3 caso PIX\n"
            "  %s q3 N T CAP        (Q3) Ctrl+C mostra a fila\n", p, p, p, p);
    exit(1);
}

int main(int argc, char *argv[]) {
    if (argc < 2) uso(argv[0]);
    const char *modo = argv[1];
#define ARG(i, padrao) (argc > (i) ? atoi(argv[i]) : (padrao))

    if (strcmp(modo, "q1") == 0) {
        int n = ARG(2, 100000), t = ARG(3, 8), s = ARG(4, 0);
        if (n < 1 || t < 1 || t > MAX_T) uso(argv[0]);
        modo_q1(n, t, s);
    } else if (strcmp(modo, "experimento") == 0) {
        int t = ARG(2, 8), r = ARG(3, 30);
        if (t < 1 || t > MAX_T || r < 1) uso(argv[0]);
        modo_experimento(t, r);
    } else if (strcmp(modo, "q2") == 0) {
        carrega_cenario_q2(ARG(2, 3));
        executa_fila(0);
    } else if (strcmp(modo, "q3") == 0) {
        int n = ARG(2, 20), t = ARG(3, 2), cap = ARG(4, 4);
        if (n < 1 || t < 1 || t > MAX_T || cap < 1) uso(argv[0]);
        carrega_carga_q3(n, t, cap);
        executa_fila(1);
    } else {
        uso(argv[0]);
    }
    return 0;
}
