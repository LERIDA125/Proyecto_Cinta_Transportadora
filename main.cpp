#include <ctime>
#include <cstdlib>
#include "sistema_logistico.h"

int main() {
    srand(time(NULL));

    //Descomentar la configuracion deseada:
    //ejecutar(1, 2, 20, "Configuracion A (1 Prod, 2 Cons)");
    //ejecutar(3, 1, 20, "Configuracion B (3 Prod, 1 Cons)");
    //ejecutar(3, 3, 20, "Configuracion C (3 Prod, 3 Cons)");
    ejecutar(3, 3, 517, "Prueba de Carga Masiva (1551 pqts)");
    //ejecutar(1, 2, 0, "Prueba de Vacuidad");
    //ejecutar(1, 2, 8, "Prueba de Saturacion (8 pqts Alta)");
    // ejecutar(1, 1, 20, "Prueba de Equidad (Anti-Starvation)");
    return 0;
}
