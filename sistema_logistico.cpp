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

        s.mtx_id.lock(); 
        p.id = ++s.id_global;
        s.producidos++;
        s.mtx_id.unlock(); 

        std::this_thread::sleep_for(std::chrono::milliseconds(90));

        s.mtx_estanteria.lock();
        s.wq.push_back(p); 
        s.mtx_estanteria.unlock();

        signal(s.hay_paquetes); 
    }
}

void consumidor(Sistema& s) {
    while (true) {
        Paquete p_transf, p_proc;
        bool transferir = false, procesar = false;

        s.mtx_estanteria.lock();

        // CONDICIÓN DE SALIDA
        if (s.fin && s.wq.empty() && s.pq.empty()) {
            s.mtx_estanteria.unlock();
            break;
        }

        long long ahora = tiempo_ms();

        // 1. EVALUAR CINTA (Prioridad: Sacar paquetes terminados)
        if (!s.pq.empty() && (ahora - s.registro_cinta[s.pq.front().id] >= 550)) {
            p_proc = s.pq.front();
            s.pq.erase(s.pq.begin()); 
            procesar = true;
        }
        
        // 2. EVALUAR ESTANTERÍA (Transferir a la cinta)
        // Mantenemos la lógica de pq.size() < 5 para no extraer de la estantería si la cinta está llena
        else if (s.pq.size() < 5 && !s.wq.empty()) {
            
            int indice_elegido = -1;
            for (size_t i = 0; i < s.wq.size(); ++i) {
                if (s.wq[i].prioridad == 0 && (ahora - s.wq[i].fecha_creacion >= 6000)) {
                    indice_elegido = i; break;
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
            transferir = true;
        }

        // 3. ESPERA PASIVA (Sin consumir CPU al 100%)
        else {
            if (s.pq.empty()) {
                s.mtx_estanteria.unlock();
                wait(s.hay_paquetes); // Se duerme hasta que un productor genere algo
            } else {
                long long faltante = 550 - (ahora - s.registro_cinta[s.pq.front().id]);
                s.mtx_estanteria.unlock();
                
                if (faltante > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(faltante));
                }
            }
            continue; // Vuelve a evaluar el bucle
        }

        s.mtx_estanteria.unlock();

        // ==========================================================
        // ACCIONES FÍSICAS (Acá integramos los semáforos del profe)
        // ==========================================================
        if (transferir) {
            wait(s.lugares_en_cinta); // Descuenta un lugar disponible (máx 5)

            s.mtx_cinta.lock();
            std::this_thread::sleep_for(std::chrono::milliseconds(420));
            s.mtx_cinta.unlock();

            s.mtx_estanteria.lock();
            s.pq.push_back(p_transf);
            s.registro_cinta[p_transf.id] = tiempo_ms();
            s.mtx_estanteria.unlock();

            signal(s.items_en_cinta); // Avisa que hay un nuevo ítem en la cinta
        }

        if (procesar) {
            wait(s.items_en_cinta); // Descuenta el ítem que estamos por procesar

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

            signal(s.lugares_en_cinta); // Libera un espacio en la cinta para otro paquete
        }
    }
}

// FUNCIÓN EJECUTAR
void ejecutar(int n_prod, int n_cons, int pqts, const std::string& nombre) {
    Sistema s;

    // Inicialización de semáforos clásicos requeridos
    init(s.hay_paquetes, 0);
    init(s.lugares_en_cinta, 5); // Arranca en 5 (Límite estricto de la cinta)
    init(s.items_en_cinta, 0);   // Arranca en 0 (Cinta vacía)

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