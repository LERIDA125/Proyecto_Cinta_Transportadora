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
    std::vector<Paquete> wq;
    std::vector<Paquete> pq;
    std::map<int, long long> registro_cinta;

    // Mutex para exclusión mutua de las estructuras
    std::mutex mtx_estanteria;
    std::mutex mtx_cinta;
    std::mutex mtx_id;

    // SEMÁFOROS OBLIGATORIOS (Estrategia Clásica)
    Semaforo hay_paquetes;      // Cuenta cuántos paquetes hay en la estantería (Inicia en 0)
    Semaforo lugares_en_cinta;  // Controla el límite estricto de 5 en la cinta (Inicia en 5)
    Semaforo items_en_cinta;    // Cuenta cuántos paquetes listos para salir hay en la cinta (Inicia en 0)

    int id_global = 0, producidos = 0, proc_alta = 0, proc_baja = 0;
    long long espera_alta = 0, espera_baja = 0;
    bool fin = false;
};

void productor(Sistema& s, int cant);
void consumidor(Sistema& s);
void ejecutar(int n_prod, int n_cons, int pqts, const std::string& nombre);

#endif
