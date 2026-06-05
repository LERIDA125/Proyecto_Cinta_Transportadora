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
        Paquete p_transf, p_proc;
        bool transferir = false, procesar = false;

        s.mtx_estanteria.lock();

        if (s.fin && s.wq.empty() && s.pq.empty()) {
            s.mtx_estanteria.unlock();
            break;
        }

        long long ahora = tiempo_ms();

        // 1. EVALUACIÓN DE LA CINTA
        if (!s.pq.empty() && (ahora - s.registro_cinta[s.pq.front().id] >= 550)) {
            p_proc = s.pq.front();
            s.pq.erase(s.pq.begin()); // Extrae limpiamente el primero (FIFO)
            procesar = true;
        }

        // 2. EVALUACIÓN DE LA ESTANTERÍA (Recorrido limpio sin pop/push)
        else if (s.pq.size() < 5 && !s.wq.empty()) {
            
            int indice_elegido = -1;

            // A. Buscar Inanición (Miro los elementos directamente)
            for (size_t i = 0; i < s.wq.size(); ++i) {
                if (s.wq[i].prioridad == 0 && (ahora - s.wq[i].fecha_creacion >= 6000)) {
                    indice_elegido = i;
                    break; 
                }
            }

            // B. Si no hay inanición, busco Prioridad Alta
            if (indice_elegido == -1) {
                for (size_t i = 0; i < s.wq.size(); ++i) {
                    if (s.wq[i].prioridad == 1) {
                        indice_elegido = i;
                        break;
                    }
                }
            }

            // C. Si no hubo nada especial, saco el primero (FIFO)
            if (indice_elegido == -1) {
                indice_elegido = 0;
            }

            // D. EXTRAER el paquete elegido
            p_transf = s.wq[indice_elegido];
            s.wq.erase(s.wq.begin() + indice_elegido); // Borra solo ese elemento
            transferir = true;
        }

        // 3. ESPERA PASIVA
        else {
            if (s.pq.empty()) {
                s.mtx_estanteria.unlock();
                wait(s.hay_paquetes); // El hilo se duerme
            } else {
                int id_primero = s.pq.front().id;
                long long faltante = 550 - (ahora - s.registro_cinta[id_primero]);
                
                s.mtx_estanteria.unlock();
                
                if (faltante > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(faltante));
                }
            }
            continue;
        }

        s.mtx_estanteria.unlock();

        // ACCIONES FÍSICAS
        if (transferir) {
            s.mtx_cinta.lock();
            std::this_thread::sleep_for(std::chrono::milliseconds(420));

            s.mtx_estanteria.lock();
            s.pq.push_back(p_transf); 
            s.registro_cinta[p_transf.id] = tiempo_ms(); 
            s.mtx_estanteria.unlock();

            s.mtx_cinta.unlock();
            signal(s.hay_paquetes);
        }

        if (procesar) {
            s.mtx_cinta.lock();
            std::this_thread::sleep_for(std::chrono::milliseconds(270));

            long long tiempo_total = tiempo_ms() - p_proc.fecha_creacion;

            s.mtx_estanteria.lock();
            s.registro_cinta.erase(p_proc.id); 

            if (p_proc.prioridad == 1) {
                s.proc_alta++; s.espera_alta += tiempo_total;
            } else {
                s.proc_baja++; s.espera_baja += tiempo_total;
            }
            s.mtx_estanteria.unlock();

            s.mtx_cinta.unlock();
        }
    }
}

// FUNCIÓN EJECUTAR
void ejecutar(int n_prod, int n_cons, int pqts, const std::string& nombre) {
    Sistema s;

    // Ya no inicializamos los mutex acá porque std::mutex se inicializa solo.
    // Solo inicializamos el semáforo contador en 0.
    init(s.hay_paquetes, 0);

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