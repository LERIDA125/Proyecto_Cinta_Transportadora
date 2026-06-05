#ifndef SISTEMA_LOGISTICO_H
#define SISTEMA_LOGISTICO_H

#include "semaforo.h"
#include <vector>
#include <map>
#include <string>
#include <mutex> // Usamos Mutex reales como pidió el profe

struct Paquete {
    int id;
    int prioridad;
    long long fecha_creacion;
};

struct Sistema {
    // Usamos arreglos dinámicos, tratados lógicamente como colas (FIFO).
    // Esto permite iterar sin hacer pop/push.
    std::vector<Paquete> wq; 
    std::vector<Paquete> pq; 

    std::map<int, long long> registro_cinta;

    // Mutex para exclusión mutua estricta de las variables compartidas
    std::mutex mtx_estanteria;
    std::mutex mtx_cinta;
    std::mutex mtx_id;
    
    // Semáforo común (contador) SOLO para la sincronización de hilos dormidos
    Semaforo hay_paquetes;

    int id_global = 0, producidos = 0, proc_alta = 0, proc_baja = 0;
    long long espera_alta = 0, espera_baja = 0;
    bool fin = false;
};

void productor(Sistema& s, int cant);
void consumidor(Sistema& s);
void ejecutar(int n_prod, int n_cons, int pqts, const std::string& nombre);

#endif