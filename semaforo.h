#ifndef SEMAFORO_H
#define SEMAFORO_H

#include <mutex>
#include <condition_variable>

struct Semaforo {
    int contador;
    std::mutex mtx;
    std::condition_variable cv;
};

void init(Semaforo& s, int n);
void wait(Semaforo& s);
void signal(Semaforo& s);

#endif
