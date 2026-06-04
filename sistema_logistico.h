#ifndef SISTEMA_LOGISTICO_H
#define SISTEMA_LOGISTICO_H

#include "semaforo.h"
#include <queue>
#include <map>
#include <string>


struct Paquete {
    int id;
    int prioridad;
    long long fecha_creacion;
};

struct Sistema {
    //"Estructuras de datos, se aconsejo usar dos colar comunes

    std::queue<Paquete> wq; // Buffer 1: Waiting Queue
    std::queue<Paquete> pq; // Buffer 2: Processing Queue

    // Para hacer mapeo en general
    std::map<int, long long> registro_cinta;

    // Semaforos
    Semaforo mtx_estanteria, mtx_cinta, mtx_id, hay_paquetes;

    int id_global = 0, producidos = 0, proc_alta = 0, proc_baja = 0;
    long long espera_alta = 0, espera_baja = 0;
    bool fin = false;
};

void productor(Sistema& s, int cant);
void consumidor(Sistema& s);
void ejecutar(int n_prod, int n_cons, int pqts, const std::string& nombre);

#endif
