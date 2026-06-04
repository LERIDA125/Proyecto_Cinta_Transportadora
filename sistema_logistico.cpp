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

        wait(s.mtx_id);
        p.id = ++s.id_global;
        s.producidos++;
        signal(s.mtx_id);

        std::this_thread::sleep_for(std::chrono::milliseconds(90));

        wait(s.mtx_estanteria);
        s.wq.push(p);
        signal(s.mtx_estanteria);

        signal(s.hay_paquetes);
    }
}

void consumidor(Sistema& s) {
    while (true) {
        Paquete p_transf;
        Paquete p_proc;
        bool transferir = false, procesar = false;

        wait(s.mtx_estanteria);

        if (s.fin && s.wq.empty() && s.pq.empty()) {
            signal(s.mtx_estanteria);
            break;
        }

        long long ahora = tiempo_ms();

        // 1 Se evalua la cinta
        if (!s.pq.empty() && (ahora - s.registro_cinta[s.pq.front().id] >= 550)) {
            p_proc = s.pq.front();
            s.pq.pop();
            procesar = true;
        }

        // Se evalua la estanteria
        else if (s.pq.size() < 5 && !s.wq.empty()) {
            int target_id = -1;
            int wq_size = s.wq.size();

            // A. Buscar Inanición (>= 6000ms) Se evalua el tiempo desde su creacion y esto
            // va como prioridad por sobre el orden, segun la consigna
            for (int i = 0; i < wq_size; i++) {
                Paquete p = s.wq.front();
                s.wq.pop();

                if (target_id == -1 && p.prioridad == 0 && (ahora - p.fecha_creacion >= 6000)) {
                    target_id = p.id;
                }
                s.wq.push(p);
            }

            // B. Buscar Prioridad Alta (1)
            if (target_id == -1) {
                for (int i = 0; i < wq_size; i++) {
                    Paquete p = s.wq.front();
                    s.wq.pop();

                    if (target_id == -1 && p.prioridad == 1) {
                        target_id = p.id;
                    }
                    s.wq.push(p);
                }
            }

            // C. Cuando no se encontro nada por Prioirdad o Inanicion se desencola de manera normal
            if (target_id == -1) {
                target_id = s.wq.front().id;
            }

            // D. EXTRAER el paquete elegido (Tercera y última rotación)
            for (int i = 0; i < wq_size; i++) {
                Paquete p = s.wq.front();
                s.wq.pop();

                if (!transferir && p.id == target_id) {
                    p_transf = p;
                    transferir = true;
                } else {
                    s.wq.push(p);
                }
            }
        }

        // 3 Espera Pasiva
        else {
            if (s.pq.empty()) {
                signal(s.mtx_estanteria);
                wait(s.hay_paquetes);
            } else {
                int id_primero = s.pq.front().id;
                long long faltante = 550 - (ahora - s.registro_cinta[id_primero]);

                signal(s.mtx_estanteria);

                if (faltante > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(faltante));
                }
            }
            continue;
        }

        signal(s.mtx_estanteria);

        // Acciones del Consumidor

        if (transferir) {
            wait(s.mtx_cinta);
            std::this_thread::sleep_for(std::chrono::milliseconds(420));

            wait(s.mtx_estanteria);
            s.pq.push(p_transf);
            s.registro_cinta[p_transf.id] = tiempo_ms();
            signal(s.mtx_estanteria);

            signal(s.mtx_cinta);
            signal(s.hay_paquetes);
        }

        if (procesar) {
            wait(s.mtx_cinta);
            std::this_thread::sleep_for(std::chrono::milliseconds(270));

            long long tiempo_total = tiempo_ms() - p_proc.fecha_creacion;

            wait(s.mtx_estanteria);
            s.registro_cinta.erase(p_proc.id);

            if (p_proc.prioridad == 1) {
                s.proc_alta++; s.espera_alta += tiempo_total;
            } else {
                s.proc_baja++; s.espera_baja += tiempo_total;
            }
            signal(s.mtx_estanteria);

            signal(s.mtx_cinta);
        }
    }
}

// Funcion Ejecutar : Se utiliza para lanzar los hilos automaticamente y dormirlos
// de modo que en el main para porbar los escenarios quede sean mas faciles de manejar pasandoles la cantidad
// de productores y consumidores
// y la cantidad de paquetes
void ejecutar(int n_prod, int n_cons, int pqts, const std::string& nombre) {
    Sistema s;

    // Inicialización de nuestros semáforos (Mutex en 1, Contador en 0)
    init(s.mtx_estanteria, 1);
    init(s.mtx_cinta, 1);
    init(s.mtx_id, 1);
    init(s.hay_paquetes, 0);

    std::vector<std::thread> h_cons, h_prod;
    std::cout << "\n======================================================\n";
    std::cout << "Ejecutando: " << nombre << "...\n";
    std::cout << "======================================================\n";

    // 1. Arrancan los hilos mediante un for
    for (int i = 0; i < n_cons; ++i) h_cons.emplace_back(consumidor, std::ref(s));
    for (int i = 0; i < n_prod; ++i) h_prod.emplace_back(productor, std::ref(s), pqts);

    // 2. Se los hace termianr su turno con .join
    for (auto& t : h_prod) t.join();

    // 3. Se avisa a los consumidores que termino el proceso modificando la variable protegida fin
    wait(s.mtx_estanteria);
    s.fin = true;
    signal(s.mtx_estanteria);

    // Despertamos a cualquier consumidor que se haya quedado dormido esperando paquetes
    for(int i = 0; i < n_cons * 5; i++) {
        signal(s.hay_paquetes);
    }

    // 4. ESPERAMOS A QUE LOS CONSUMIDORES TERMINEN DE VACIAR LAS COLAS
    for (auto& t : h_cons) t.join();

    // 5. Calculo e impresion de las Metricas
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
