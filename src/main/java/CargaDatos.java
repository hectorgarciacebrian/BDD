import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

public class CargaDatos {

    // DEFINICIÓN DE NODOS (SEGÚN TU ESQUEMA CERVEZA1..4)
    private static OperacionesBD opsMadrid; // Conecta a cerveza1
    private static OperacionesBD opsBcn;    // Conecta a cerveza2
    private static OperacionesBD opsCoruna; // Conecta a cerveza3
    private static OperacionesBD opsSur;    // Conecta a cerveza4

    public static void main(String[] args) {
        System.out.println("--- INICIANDO CARGA MASIVA DE DATOS (cerveza1..cerveza4) ---");
        
        // 1. INICIALIZACIÓN DE GESTORES
        // Asegúrate de que DatabaseManager apunte a los usuarios correctos
        opsMadrid  = new OperacionesBD(DatabaseManager.Delegacion.MADRID);    // cerveza1
        opsBcn     = new OperacionesBD(DatabaseManager.Delegacion.BARCELONA); // cerveza2
        opsCoruna  = new OperacionesBD(DatabaseManager.Delegacion.CORUNA);    // cerveza3
        opsSur     = new OperacionesBD(DatabaseManager.Delegacion.SEVILLA);   // cerveza4

        // Array para replicación de Productores
        OperacionesBD[] todosLosNodos = { opsMadrid, opsBcn, opsCoruna, opsSur };
        Map<Integer, String> mapaSucursalCCAA = new HashMap<>();

        try {
            // =========================================================================
            // 1. PRODUCTORES (REPLICADOS EN TODOS: cerveza1, 2, 3 y 4)
            // =========================================================================
            System.out.println("\n1. Insertando Productores...");
            Object[][] productores = {
                {1, "35353535A", "Justiniano Briñón", "Ramón y Cajal 9, Valladolid"},
                {2, "36363636B", "Marcelino Peña", "San Francisco 7, Pamplona"},
                {3, "37373737C", "Paloma Riquelme", "Antonio Gaudí 23, Barcelona"},
                {4, "38383838D", "Amador Laguna", "Juan Ramón Jiménez 17, Córdoba"},
                {5, "39393939E", "Ramón Esteban", "Gran Vía de Colón 121, Madrid"},
                {6, "40404040F", "Carlota Fuentes", "Cruz de los Ángeles 29, Oviedo"}
            };

            for (Object[] p : productores) {
                for (OperacionesBD nodo : todosLosNodos) {
                    try {
                        nodo.altaProductor((int)p[0], (String)p[1], (String)p[2], (String)p[3]);
                    } catch (Exception e) { /* Ignorar duplicados */ }
                }
            }

            // =========================================================================
            // 2. SUCURSALES (FRAGMENTADAS POR CCAA)
            // =========================================================================
            System.out.println("2. Insertando Sucursales...");
            Object[][] sucursales = {
                // {Código, Nombre, Ciudad, CCAA, Director}
                {1, "Santa Cruz", "Sevilla", "Andalucía", 1},          // va a cerveza4
                {2, "Palacios Nazaríes", "Granada", "Andalucía", 3},   // va a cerveza4
                {3, "Tacita de Plata", "Cádiz", "Andalucía", 5},       // va a cerveza4
                {4, "Almudena", "Madrid", "Madrid", 7},                // va a cerveza1
                {5, "El Cid", "Burgos", "Castilla-León", 9},           // va a cerveza1
                {6, "Puente la Reina", "Logroño", "La Rioja", 11},     // va a cerveza1
                {7, "Catedral del Mar", "Barcelona", "Cataluña", 13},  // va a cerveza2
                {8, "Dama de Elche", "Alicante", "País Valenciano", 15},// va a cerveza2
                {9, "La Cartuja", "Palma de Mallorca", "Baleares", 17},// va a cerveza2
                {10, "Meigas", "La Coruña", "Galicia", 19},            // va a cerveza3
                {11, "La Concha", "San Sebastián", "País Vasco", 21},  // va a cerveza3
                {12, "Don Pelayo", "Oviedo", "Asturias", 23}           // va a cerveza3
            };

            for (Object[] s : sucursales) {
                int cod = (int)s[0];
                String ca = (String)s[3];
                mapaSucursalCCAA.put(cod, ca); 

                try {
                    // getOpsPorCCAA selecciona cerveza1, 2, 3 o 4 según la CA
                    getOpsPorCCAA(ca).altaSucursal(cod, (String)s[1], (String)s[2], ca, null);
                } catch (Exception e) {}
            }

            // =========================================================================
            // 3. EMPLEADOS (FRAGMENTADOS AL NODO DE SU SUCURSAL)
            // =========================================================================
            System.out.println("3. Insertando Empleados...");
            // {Cod, DNI, Nombre, Fecha, Salario, Direccion, CodSucursal}
            Object[][] empleados = {
                {1, "11111111A", "Raúl", "2010-09-21", 2000, "Sierpes 37, Sevilla", 1},
                {2, "22222222B", "Federico", "2009-08-25", 1800, "Emperatriz 25, Sevilla", 1},
                {3, "33333333C", "Natalia", "2012-01-30", 2000, "Ronda 126, Granada", 2},
                {4, "44444444D", "Amalia", "2013-02-13", 1800, "San Matías 23, Granada", 2},
                {5, "55555555E", "Susana", "2018-10-01", 2000, "Zoraida 5, Cádiz", 3},
                {6, "66666666F", "Gonzalo", "2007-01-01", 1800, "Tartesios 9, Cádiz", 3},
                {7, "77777777G", "Agustín", "2019-05-05", 2000, "Pablo Neruda 84, Madrid", 4},
                {8, "88888888H", "Eduardo", "2019-06-06", 1800, "Alcalá 8, Madrid", 4},
                {9, "99999999I", "Alberto", "2020-09-05", 2000, "Las Huelgas 15, Burgos", 5},
                {10, "10101010J", "Soraya", "2017-10-04", 1800, "Jimena 2, Burgos", 5},
                {11, "01010101K", "Manuel", "2016-07-06", 2000, "Estrella 26, Logroño", 6},
                {12, "12121212L", "Emilio", "2018-11-05", 1800, "Constitución 3, Logroño", 6},
                {13, "13131313M", "Patricia", "2019-12-04", 2000, "Diagonal 132, Barcelona", 7},
                {14, "14141414N", "Inés", "2018-03-07", 1800, "Colón 24, Barcelona", 7},
                {15, "15151515O", "Carlos", "2019-06-16", 2000, "Palmeras 57, Alicante", 8},
                {16, "16161616P", "Dolores", "2018-05-14", 1800, "Calatrava 9, Alicante", 8},
                {17, "17171717Q", "Elías", "2019-06-13", 2000, "Arenal 17, P. Mallorca", 9},
                {18, "18181818R", "Concepción", "2020-08-01", 1800, "Campastilla 14, P. Mallorca", 9},
                {19, "19191919S", "Gabriel", "2015-09-19", 2000, "Hércules 19, La Coruña", 10},
                {20, "20202020T", "Octavio", "2017-10-20", 1800, "María Pita 45, La Coruña", 10},
                {21, "21212121V", "Cesar", "2021-11-13", 2000, "Las Peñas 41, San Sebastián", 11},
                {23, "24242424X", "Claudia", "2022-02-13", 2000, "Santa Cruz 97, Oviedo", 12},
                {24, "25252525Z", "Mario", "2017-04-23", 1800, "Naranco 21, Oviedo", 12}
            };
            for (Object[] e : empleados) {
                try {
                    int suc = (int)e[6];
                    String caSucursal = mapaSucursalCCAA.get(suc);
                    getOpsPorCCAA(caSucursal).altaEmpleado(
                        (int)e[0], (String)e[1], (String)e[2], Date.valueOf((String)e[3]), 
                        ((Integer)e[4]).doubleValue(), (String)e[5], suc
                    );
                } catch (Exception ex) {}
            }

            // 4. Asignar Directores (UPDATE local)
            System.out.println("4. Asignando Directores...");
            for (Object[] s : sucursales) {
                try {
                    getOpsPorCCAA((String)s[3]).cambiarDirector((int)s[0], (int)s[4]);
                } catch (Exception e) {}
            }

            // =========================================================================
            // 5. VINOS (FRAGMENTADOS - SOLO EN SU ORIGEN)
            // =========================================================================
            System.out.println("5. Insertando Vinos...");
            Object[][] vinos = {
                // {Cod, Nombre, Año, DO, Grad, Viñedo, CCAA, Prod, Productor}
                {1, "Vega Sicilia", 2008, "Ribera del Duero", 12.5, "Castillo Blanco", "Castilla-León", 200, 1},
                {2, "Vega Sicilia", 2015, "Ribera del Duero", 12.5, "Castillo Blanco", "Castilla-León", 100, 1},
                {3, "Marqués de Cáceres", 2019, "Rioja", 11.0, "Santo Domingo", "La Rioja", 200, 2},
                {4, "Marqués de Cáceres", 2022, "Rioja", 11.5, "Santo Domingo", "La Rioja", 250, 2},
                {5, "René Barbier", 2023, "Penedés", 11.5, "Virgen de Estrella", "Cataluña", 200, 3},
                {6, "René Barbier", 2020, "Penedés", 11.0, "Virgen de Estrella", "Cataluña", 250, 3},
                {7, "Rias Baixas", 2024, "Albariño", 9.5, "Santa Compaña", "Galicia", 150, 4},
                {8, "Rias Baixas", 2023, "Albariño", 9.0, "Santa Compaña", "Galicia", 100, 4},
                {9, "Córdoba Bella", 2018, "Montilla", 12.0, "Mezquita Roja", "Andalucía", 200, 4},
                {10, "Tío Pepe", 2020, "Jerez", 12.5, "Campo Verde", "Andalucía", 200, 4},
                {13, "Vega Murciana", 2023, "Jumilla", 11.5, "Vega Verde", "Murcia", 250, 5},
                {14, "Tablas de Daimiel", 2018, "Valdepeñas", 11.5, "Laguna Azul", "Castilla-La Mancha", 300, 5},
                {15, "Santa María", 2023, "Tierra de Cangas", 10.0, "Monte Astur", "Asturias", 200, 6},
                {16, "Freixenet", 2024, "Cava", 7.5, "Valle Dorado", "Cataluña", 250, 6},
                {17, "Estela", 2022, "Cariñena", 10.5, "San Millán", "Aragón", 200, 3},
                {18, "Uva dorada", 2023, "Málaga", 13.0, "Axarquía", "Andalucía", 200, 5},
                {19, "Meigas Bellas", 2024, "Ribeiro", 8.5, "Mayor Santiago", "Galicia", 250, 6},
                {20, "Altamira", 2024, "Tierra de Liébana", 9.5, "Cuevas", "Cantabria", 300, 1},
                {21, "Virgen negra", 2024, "Islas Canarias", 10.5, "Guanche", "Canarias", 300, 3}
            };
            for (Object[] v : vinos) {
                try {
                    String ca = (String)v[6];
                    getOpsPorCCAA(ca).altaVino(
                        (int)v[0], (String)v[1], (int)v[2], (String)v[3], 
                        (double)v[4], (String)v[5], ca, (int)v[7], (int)v[8]
                    );
                } catch (Exception e) {}
            }

            // =========================================================================
            // 6. CLIENTES (FRAGMENTADOS POR CCAA)
            // =========================================================================
            System.out.println("6. Insertando Clientes...");
            Object[][] clientes = {
                {1, "26262626A", "Hipercor", "Jaén", "A", "Andalucía"},
                {2, "27272727B", "Restaurante Cacereño", "Cáceres", "C", "Extremadura"},
                {3, "28282828C", "Continente", "Vigo", "A", "Galicia"},
                {4, "29292929D", "Restaurante El Asturiano", "Luarca", "C", "Asturias"},
                {5, "30303030E", "Restaurante El Payés", "Mahón", "C", "Baleares"},
                {6, "31313131F", "Mercadona", "Castellón", "A", "País Valenciano"},
                {7, "32323232G", "Restaurante Cándido", "Segovia", "C", "Castilla-León"},
                {8, "34343434H", "Restaurante Las Vidrieras", "Almagro", "C", "Castilla-La Mancha"}
            };
            for (Object[] c : clientes) {
                try {
                    String ca = (String)c[5];
                    getOpsPorCCAA(ca).altaCliente(
                        (int)c[0], (String)c[1], (String)c[2], (String)c[3], (String)c[4], ca
                    );
                } catch (Exception e) {}
            }

            // =========================================================================
            // 7. SUMINISTROS (Tabla PIDE - Solicitudes de Clientes)
            // =========================================================================
            System.out.println("7. Gestionando Suministros (Peticiones Clientes - Tabla PIDE)...");
            // {CodCliente, CodSucursal, CodVino, Fecha, Cantidad, CCAA}
            Object[][] suministros = {
                // Clientes Andalucía (cerveza4)
                {1, 1, 4, "2025-06-12", 100, "Andalucía"},
                {1, 2, 5, "2025-07-11", 150, "Andalucía"},
                {1, 3, 14, "2025-07-15", 200, "Andalucía"},
                // Clientes Extremadura (cerveza4)
                {2, 2, 2, "2025-04-03", 20, "Extremadura"},
                {2, 1, 7, "2025-05-04", 50, "Extremadura"},
                {2, 2, 6, "2025-09-15", 40, "Extremadura"},
                {2, 3, 16, "2025-09-20", 100, "Extremadura"},
                // Clientes Galicia (cerveza3)
                {3, 10, 3, "2025-02-21", 100, "Galicia"},
                {3, 10, 6, "2025-08-02", 90, "Galicia"},
                {3, 11, 13, "2025-10-03", 200, "Galicia"},
                {3, 12, 20, "2025-11-04", 150, "Galicia"},
                // Clientes Asturias (cerveza3)
                {4, 12, 8, "2025-03-01", 50, "Asturias"},
                {4, 12, 17, "2025-05-03", 70, "Asturias"},
                // Clientes Baleares (cerveza2)
                {5, 7, 16, "2025-08-14", 50, "Baleares"},
                {5, 9, 18, "2025-10-01", 100, "Baleares"},
                // Clientes País Valenciano (cerveza2)
                {6, 8, 15, "2025-01-13", 100, "País Valenciano"},
                {6, 8, 9, "2025-02-19", 150, "País Valenciano"},
                {6, 9, 19, "2025-06-27", 160, "País Valenciano"},
                {6, 7, 21, "2025-09-17", 200, "País Valenciano"},
                // Clientes Castilla-León (cerveza1)
                {7, 4, 1, "2025-02-15", 80, "Castilla-León"},
                {7, 5, 7, "2025-04-17", 50, "Castilla-León"},
                {7, 4, 10, "2025-06-21", 70, "Castilla-León"},
                //{7, 5, 12, "2025-07-23", 40, "Castilla-León"},
                // Clientes Castilla-La Mancha (cerveza1)
                {8, 6, 14, "2025-01-11", 50, "Castilla-La Mancha"},
                {8, 6, 4, "2025-03-14", 60, "Castilla-La Mancha"},
                {8, 4, 6, "2025-05-21", 70, "Castilla-La Mancha"}
            };
            
            for (Object[] s : suministros) {
                try {
                    String caDest = (String)s[5];
                    getOpsPorCCAA(caDest).gestionarSuministro(
                        (int)s[0], (int)s[1], (int)s[2], Date.valueOf((String)s[3]), (int)s[4]
                    );
                } catch (Exception e) { 
                    System.err.println("Error Suministro Cli " + s[0] + " Vino " + s[2] + ": " + e.getMessage()); 
                }
            }

            // =========================================================================
            // 8. PEDIDOS ENTRE SUCURSALES (Tabla SOLICITA)
            // =========================================================================
            System.out.println("8. Realizando Pedidos entre Sucursales (Tabla SOLICITA)...");
            // {SucSolicitante, SucProveedora, Vino, Fecha, Cantidad}
            Object[][] pedidos = {
                // Sucursal 1 (Sevilla)
                {1, 4, 4, "2025-06-13", 100}, // R18 OK: Cliente pidió 100+60. Fecha OK > 12-06
                {1, 10, 7, "2025-05-05", 50}, // R18 OK: Cliente pidió 50. Fecha OK > 04-05
                // Sucursal 2 (Granada)
                {2, 7, 5, "2025-07-12", 150}, 
                {2, 5, 2, "2025-04-04", 20},  
                {2, 8, 6, "2025-09-16", 40},  
                // Sucursal 3 (Cádiz)
                {3, 6, 14, "2025-07-15", 200},
                {3, 9, 16, "2025-09-21", 100},
                // Sucursal 4 (Madrid)
                {4, 1, 10, "2025-06-22", 70}, 
                {4, 7, 6, "2025-05-22", 70},
                // Sucursal 5 (Burgos)
                {5, 10, 7, "2025-04-18", 50},
                // Sucursal 7 (Barcelona)
                {7, 2, 21, "2025-09-18", 200},
                // Sucursal 8 (Alicante)
                {8, 11, 15, "2025-01-14", 100},
                {8, 2, 9, "2025-02-20", 150},
                // Sucursal 9 (Palma)
                {9, 3, 18, "2025-10-02", 100},
                {9, 12, 19, "2025-06-28", 160},
                // Sucursal 10 (Coruña)
                {10, 4, 3, "2025-02-22", 100},
                {10, 8, 6, "2025-08-02", 90},
                // Sucursal 11 (San Sebastián)
                {11, 9, 13, "2025-10-04", 200},
                // Sucursal 12 (Oviedo)
                {12, 4, 17, "2025-05-04", 70}
            };
            
            for (Object[] p : pedidos) {
                try {
                    int sucPide = (int)p[0];
                    String caPide = mapaSucursalCCAA.get(sucPide);
                    
                    // Ejecutar en el nodo de la sucursal que pide
                    getOpsPorCCAA(caPide).altaPedidoSucursal(
                        (int)p[0], (int)p[1], (int)p[2], Date.valueOf((String)p[3]), (int)p[4]
                    );
                    System.out.println("   -> Pedido OK: Suc " + p[0] + " pide a " + p[1]);
                } catch (Exception e) { 
                    System.err.println("Error Pedido Suc " + p[0] + " Vino " + p[2] + ": " + e.getMessage()); 
                }
            }

        } catch (Exception e) {
            System.err.println("Error CRÍTICO en carga: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // --- ENRUTADOR DE COMUNIDADES A ESQUEMAS CERVEZA1..4 ---
    private static OperacionesBD getOpsPorCCAA(String ccaa) {
        if (ccaa == null) return opsMadrid;

        switch (ccaa) {
            // CERVEZA 1 (CENTRO)
            case "Castilla-León": 
            case "Castilla-La Mancha": 
            case "Aragón": 
            case "Madrid": 
            case "La Rioja":
                return opsMadrid;

            // CERVEZA 2 (LEVANTE)
            case "Cataluña": 
            case "Baleares": 
            case "País Valenciano": 
            case "Murcia":
                return opsBcn;

            // CERVEZA 3 (NORTE)
            case "Galicia": 
            case "Asturias": 
            case "Cantabria": 
            case "País Vasco": 
            case "Navarra":
                return opsCoruna;

            // CERVEZA 4 (SUR)
            case "Andalucía": 
            case "Extremadura": 
            case "Canarias": 
            case "Ceuta": 
            case "Melilla":
                return opsSur;

            default:
                // Por defecto a Madrid si no se encuentra
                System.out.println("AVISO: CCAA " + ccaa + " no mapeada. Usando cerveza1.");
                return opsMadrid;
        }
    }
}