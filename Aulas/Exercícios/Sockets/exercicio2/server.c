#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>

#define PORT 1101

struct CalcProtocol {
    char operation; // '+', '-', '*', '/'
    double op1;
    double op2;
};

double calcular(struct CalcProtocol calc) {
    switch(calc.operation) {
        case '+': return calc.op1 + calc.op2;
        case '-': return calc.op1 - calc.op2;
        case '*': return calc.op1 * calc.op2;
        case '/': 
            if(calc.op2 != 0) return calc.op1 / calc.op2;
            else return 0; // Tratar erro de divisão por zero
        default: return 0;
    }
}

int main() {
    int server_sockfd, client_sockfd;
    int server_len, client_len;
    struct sockaddr_in server_address;
    struct sockaddr_in client_address;

    struct CalcProtocol request;
    double result;

    server_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(PORT);

    server_len = sizeof(server_address);
    
    // associando o socket ao endereço
    bind(server_sockfd, (struct sockaddr *)&server_address, server_len);
    listen(server_sockfd, 5);

    // soma, subtração, multiplicação e divisão


    while(1) {
        printf("server waiting\n");
        client_len = sizeof(client_address);
        client_sockfd = accept(server_sockfd,(struct sockaddr *)&client_address, &client_len);
        read(client_sockfd, &request, sizeof(struct CalcProtocol));

        printf("request: %c\n", request.operation);
        printf("request: %f\n", request.op1);
        printf("request: %f\n", request.op2);

        result = calcular(request);

        printf("result: %f\n", result);

        write(client_sockfd, &result, sizeof(double));
        close(client_sockfd);
    }
}