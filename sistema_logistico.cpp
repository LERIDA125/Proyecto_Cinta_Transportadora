#include "sistema_logistico.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cstdlib>

long long tiempo_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

void productor(Sistema& s, int cant) {
    for (int i = 0; i < cant; ++i) {
        Paquete p;
        p.prioridad = rand() % 2;
        p.fecha_creacion = tiempo_ms();

        s.mtx_id.lock(); // Uso de Mutex en vez de wait
        p.id = ++s.id_global;
        s.producidos++;
        s.mtx_id.unlock(); // Uso de Mutex en vez de signal

        std::this_thread::sleep_for(std::chrono::milliseconds(90));

        s.mtx_estanteria.lock();
        s.wq.push_back(p); // Ingresa al final del arreglo (Cola)
        s.mtx_estanteria.unlock();

        signal(s.hay_paquetes); // Semáforo contador para avisar
    }
}

void consumidor(Sistema& s) {
    while (true) {
        // CONDICIÓN DE SALIDA
        s.mtx_estanteria.lock();
        if (s.fin && s.wq.empty() && s.pq.empty()) {
            s.mtx_estanteria.unlock();
            break;
        }
        s.mtx_estanteria.unlock();

        // 1. INTENTAR TRANSFERIR (De Estantería a Cinta)
        // Solo entramos si hay paquetes en la estantería Y hay lugar en la cinta
        // Esto elimina el "if (s.pq.size() < 5)" problemático.

        bool puede_procesar = false;
        Paquete p_proc;

        bool puede_transferir = false;
        Paquete p_transf;

        long long ahora = tiempo_ms();

        // Una forma segura de testear sin bloquearse eternamente si no hay elementos:
        s.mtx_estanteria.lock();
        if (!s.wq.empty() && s.pq.size() < 5) {
            // Busco prioridad o inanición (Tu algoritmo actual)
            int indice_elegido = -1;
            for (size_t i = 0; i < s.wq.size(); ++i) {
                if (s.wq[i].prioridad == 0 && (ahora - s.wq[i].fecha_creacion >= 6000)) {
                    indice_elegido = i;
                    break;
                }
            }
            if (indice_elegido == -1) {
                for (size_t i = 0; i < s.wq.size(); ++i) {
                    if (s.wq[i].prioridad == 1) { indice_elegido = i; break; }
                }
            }
            if (indice_elegido == -1) indice_elegido = 0;

            p_transf = s.wq[indice_elegido];
            s.wq.erase(s.wq.begin() + indice_elegido);
            puede_transferir = true;
        }
        s.mtx_estanteria.unlock();

        if (puede_transferir) {
            // Simulación física de la transferencia (Retardo de 420ms obligatorio)
            s.mtx_cinta.lock();
            std::this_thread::sleep_for(std::chrono::milliseconds(420));
            s.mtx_cinta.unlock();

            s.mtx_estanteria.lock();
            s.pq.push_back(p_transf);
            s.registro_cinta[p_transf.id] = tiempo_ms();
            s.mtx_estanteria.unlock();
        }

        // 2. INTENTAR PROCESAR (Sacar de la Cinta)

        s.mtx_estanteria.lock();
        if (!s.pq.empty()) {
            long long tiempo_en_cinta = ahora - s.registro_cinta[s.pq.front().id];
            if (tiempo_en_cinta >= 550) { // Cumple el mínimo de 550ms
                p_proc = s.pq.front();
                s.pq.erase(s.pq.begin());
                puede_procesar = true;
            }
        }
        s.mtx_estanteria.unlock();

        if (puede_procesar) {
            // Retardo físico de retiro (270ms obligatorio)
            s.mtx_cinta.lock();
            std::this_thread::sleep_for(std::chrono::milliseconds(270));
            s.mtx_cinta.unlock();

            long long tiempo_total = tiempo_ms() - p_proc.fecha_creacion;

            s.mtx_estanteria.lock();
            s.registro_cinta.erase(p_proc.id);
            if (p_proc.prioridad == 1) {
                s.proc_alta++; s.espera_alta += tiempo_total;
            } else {
                s.proc_baja++; s.espera_baja += tiempo_total;
            }
            s.mtx_estanteria.unlock();
        }

        // 3. ESPERA PASIVA CONTROLADA
        // Si en este ciclo no pudimos ni transferir ni procesar, cedemos el paso
        // para no generar un bucle de consumo de CPU al 100% (Espera activa)
        if (!puede_transferir && !puede_procesar) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

// FUNCIÓN EJECUTAR
void ejecutar(int n_prod, int n_cons, int pqts, const std::string& nombre) {
    Sistema s;

    // Inicialización de semáforos según la lógica de la cátedra
    init(s.hay_paquetes, 0);
    init(s.lugares_en_cinta, 5); // Máximo 5 paquetes simultáneos
    init(s.items_en_cinta, 0);

    std::vector<std::thread> h_cons, h_prod;
    std::cout << "\n======================================================\n";
    std::cout << "Ejecutando: " << nombre << "...\n";
    std::cout << "======================================================\n";

    for (int i = 0; i < n_cons; ++i) h_cons.emplace_back(consumidor, std::ref(s));
    for (int i = 0; i < n_prod; ++i) h_prod.emplace_back(productor, std::ref(s), pqts);

    for (auto& t : h_prod) t.join();

    s.mtx_estanteria.lock();
    s.fin = true;
    s.mtx_estanteria.unlock();

    for(int i = 0; i < n_cons * 5; i++) {
        signal(s.hay_paquetes);
    }

    for (auto& t : h_cons) t.join();

    std::cout << "Metricas obligatorias:\n"
              << "- Cantidad total de paquetes producidos: " << s.producidos << "\n";

    if (s.proc_alta > 0) {
        std::cout << "- Tiempo promedio de espera (Alta Prioridad): " << (s.espera_alta / s.proc_alta) << " ms\n";
    } else {
        std::cout << "- Tiempo promedio de espera (Alta Prioridad): N/A (0 procesados)\n";
    }

    if (s.proc_baja > 0) {
        std::cout << "- Tiempo promedio de espera (Baja Prioridad): " << (s.espera_baja / s.proc_baja) << " ms\n";
    } else {
        std::cout << "- Tiempo promedio de espera (Baja Prioridad): N/A (0 procesados)\n";
    }
    std::cout << "======================================================\n";
}
