#include "semaforo.h"
#include <chrono>

void init(Semaforo& s, int n) {
    s.contador = n;
}

void wait(Semaforo& s) {
    std::unique_lock<std::mutex> l(s.mtx);
    while (s.contador == 0) s.cv.wait(l);
    s.contador--;
}

void signal(Semaforo& s) {
    std::unique_lock<std::mutex> l(s.mtx);
    s.contador++;
    s.cv.notify_one();
}

