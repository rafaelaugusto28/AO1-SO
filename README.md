# AO1- Sistemas Operativos: Simulação de PIX com Threads

**Autores:** [RAFAEL AUGUSTO DIAS BATISTA- 2024018 ,LUCAS BOCLIN CUNHA BORGES-202401803 , LUCAS ALMEIDA OLIVEIRA ISAAC - 202401802]

**Disciplina:** INF0336 – Sistemas Operacionais

## 📌 Sobre o Projeto
Este projeto consiste numa simulação do sistema de pagamentos PIX escrita em C, em que os envios de dinheiro são atendidos por uma *thread pool* que acede a uma base de contas partilhada. O objetivo principal é implementar de forma prática os conceitos de concorrência, sincronização, gestão de filas e tratamento de sinais interprocessos abordados na disciplina.

## 🚀 Funcionalidades e Questões Implementadas

### Questão 1: Sincronismo e Condição de Corrida (5,0p)
- **Implementação:** Modelo N para 1, em que N utilizadores enviam dinheiro para a mesma conta recebedora através de uma *thread pool* (T threads fixas libertadas por um semáforo de largada).
- **Sincronização:** Utilização de **Mutex POSIX** para proteger a secção crítica (leitura e alteração do saldo). O uso de *lock/unlock* garante a exclusão mútua e evita a perda de dinheiro gerada pela condição de corrida (vista nas execuções sem sincronismo)[cite: 11].
- **Prevenção de Deadlocks:** As contas são travadas sempre na mesma ordem, quebrando a condição de espera circular[cite: 11].

### Questão 2: Escalonador FCFS (First-Come, First-Served) (3,0p)
- **Implementação:** Adicionado um mecanismo de proteção para quando chegam mais transferências do que threads livres. As requisições excedentes são organizadas por um escalonador FCFS (First-Come, First-Served)[cite: 11].
- **Fila e Semáforos:** A fila FIFO é baseada no problema do produtor-consumidor com um *buffer* limitado[cite: 11]. São utilizados três semáforos (vazio, cheio e mutex_fila) para proteger a fila contra o sobrecarregamento do servidor e gerir os envios à *pool*[cite: 11].

### Questão 3: Debugging por Sinais (2,0p)
- **Implementação:** Adição de interrupções por sinais interprocessos. O comando `Ctrl+C` (`SIGINT`) foi associado a um tratador personalizado (através da função `signal`)[cite: 11].
- **Thread Dedicada:** Em vez de interromper o processamento principal, os sinais `SIGINT` e `SIGUSR1` são bloqueados na função *main* com `pthread_sigmask`[cite: 11]. Isto garante que o sinal seja recebido exclusivamente por uma thread de depuração dedicada, que exibe o estado atual e as posições da fila FCFS em segurança[cite: 11].

## 🎥 Demonstração em Vídeos
Os vídeos com a compilação, validação e execução detalhada das três questões encontram-se disponíveis no link abaixo:

🔗 **[https://drive.google.com/drive/folders/1myNHdpieLM8szLrrc_CMHyfkk0exBwl8?usp=sharing]**


## ⚙️ Como compilar e executar (Ambiente Linux / WSL)

### 1. Compilar o código
O sistema foi desenvolvido em C para ambientes Linux/POSIX. Para compilar gerando o executável `pix.out`, execute[cite: 11]:
```bash
gcc -Wall -O2 -pthread -o pix.out pix.c
