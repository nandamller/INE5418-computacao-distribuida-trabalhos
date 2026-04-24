#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>

#define PORT 1101

struct CalcProtocol {
    char operation;
    double op1;
    double op2;
};

int main() {
    int sockfd;
    int len;
    
    struct sockaddr_in address;

    struct CalcProtocol my_req;
    double response;
    int result;

    printf("Enter operation (+, -, *, /): ");
    scanf(" %c", &my_req.operation);
    printf("Enter two numbers: ");
    scanf("%lf %lf", &my_req.op1, &my_req.op2);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr("127.0.0.1");
    address.sin_port = htons(PORT);
    len = sizeof(address);
    result = connect(sockfd, (struct sockaddr *)&address, len);
    
    if(result == -1) {
        perror("oops: client1");
        exit(1);
    }

    write(sockfd, &my_req, sizeof(struct CalcProtocol));
    read(sockfd, &response, sizeof(double));

    printf("Result from server: %lf\n", response);

    close(sockfd);
    exit(0);
}