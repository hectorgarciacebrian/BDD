import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

public class CargaDatos {

    // Variables estáticas para los gestores de cada nodo
    private static OperacionesBD opsMadrid;
    private static OperacionesBD opsBcn;
    private static OperacionesBD opsCoruna;
    private static OperacionesBD opsSur;

    public static void main(String[] args) {
        System.out.println("--- INICIANDO CARGA MASIVA (SOLO PRODUCTORES REPLICADOS) ---");
        
        // 1. Inicializamos los gestores
        opsMadrid  = new OperacionesBD(DatabaseManager.Delegacion.MADRID);
        opsBcn     = new OperacionesBD(DatabaseManager.Delegacion.BARCELONA);
        opsCoruna  = new OperacionesBD(DatabaseManager.Delegacion.CORUNA);
        opsSur     = new OperacionesBD(DatabaseManager.Delegacion.SEVILLA); 

        // Array para replicación (SOLO SE USA PARA PRODUCTORES)
        OperacionesBD[] todosLosNodos = { opsMadrid, opsBcn, opsCoruna, opsSur };

        // Mapa auxiliar para saber la ubicación de cada sucursal
        Map<Integer, String> mapaSucursalCCAA = new HashMap<>();

        try {
            // =========================================================================
            // 1. PRODUCTORES (REPLICADOS EN TODOS LOS NODOS)
            // =========================================================================
            System.out.println("\n1. Insertando Productores (Replicados en los 4 nodos)...");
            Object[][] productores = {
                {1, "35353535A", "Justiniano Briñón", "Ramón y Cajal 9, Valladolid"},
                {2, "36363636B", "Marcelino Peña", "San Francisco 7, Pamplona"},
                {3, "37373737C", "Paloma Riquelme", "Antonio Gaudí 23, Barcelona"},
                {4, "38383838D", "Amador Laguna", "Juan Ramón Jiménez 17, Córdoba"},
                {5, "39393939E", "Ramón Esteban", "Gran Vía de Colón 121, Madrid"},
                {6, "40404040F", "Carlota Fuentes", "Cruz de los Ángeles 29, Oviedo"}
            };

            for (Object[] p : productores) {
                // Bucle para insertar en TODOS los nodos
                for (OperacionesBD nodo : todosLosNodos) {
                    try {
                        nodo.altaProductor((int)p[0], (String)p[1], (String)p[2], (String)p[3]);
                    } catch (Exception e) { /* Ignorar si ya existe */ }
                }
            }

            // =========================================================================
            // 2. SUCURSALES (FRAGMENTADAS - SOLO EN SU ORIGEN)
            // =========================================================================
            System.out.println("2. Insertando Sucursales...");
            Object[][] sucursales = {
                {1, "Santa Cruz", "Sevilla", "Andalucía", 1},
                {2, "Palacios Nazaríes", "Granada", "Andalucía", 3},
                {3, "Tacita de Plata", "Cádiz", "Andalucía", 5},
                {4, "Almudena", "Madrid", "Madrid", 7},
                {5, "El Cid", "Burgos", "Castilla-León", 9},
                {6, "Puente la Reina", "Logroño", "La Rioja", 11},
                {7, "Catedral del Mar", "Barcelona", "Cataluña", 13},
                {8, "Dama de Elche", "Alicante", "País Valenciano", 15},
                {9, "La Cartuja", "Palma de Mallorca", "Baleares", 17},
                {10, "Meigas", "La Coruña", "Galicia", 19},
                {11, "La Concha", "San Sebastián", "País Vasco", 21},
                {12, "Don Pelayo", "Oviedo", "Asturias", 23}
            };

            for (Object[] s : sucursales) {
                int cod = (int)s[0];
                String ca = (String)s[3];
                mapaSucursalCCAA.put(cod, ca); // Guardamos dato para usar luego

                // SOLO insertamos en el nodo correspondiente
                try {
                    getOpsPorCCAA(ca).altaSucursal(cod, (String)s[1], (String)s[2], ca, null);
                } catch (Exception e) {
                    System.err.println("Error Sucursal " + cod + ": " + e.getMessage());
                }
            }

            // =========================================================================
            // 3. EMPLEADOS (FRAGMENTADOS - AL NODO DE SU SUCURSAL)
            // =========================================================================
            System.out.println("3. Insertando Empleados...");
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
                {22, "23232323W", "Julia", "2020-03-24", 1800, "San Cristóbal 5, San Sebastián", 11},
                {23, "24242424X", "Claudia", "2022-02-13", 2000, "Santa Cruz 97, Oviedo", 12},
                {24, "25252525Z", "Mario", "2017-04-23", 1800, "Naranco 21, Oviedo", 12}
            };

            for (Object[] e : empleados) {
                try {
                    int suc = (int)e[6];
                    String caSucursal = mapaSucursalCCAA.get(suc);
                    
                    // Fragmentación: Insertar solo en el nodo de la sucursal
                    getOpsPorCCAA(caSucursal).altaEmpleado(
                        (int)e[0], (String)e[1], (String)e[2], Date.valueOf((String)e[3]), 
                        ((Integer)e[4]).doubleValue(), (String)e[5], suc
                    );
                } catch (Exception ex) { System.err.println("Error Empleado " + e[0]); }
            }

            // =========================================================================
            // 4. DIRECTORES (FRAGMENTADOS)
            // =========================================================================
            System.out.println("4. Asignando Directores...");
            for (Object[] s : sucursales) {
                try {
                    int codSuc = (int)s[0];
                    int codDir = (int)s[4]; 
                    String ca = (String)s[3];
                    
                    getOpsPorCCAA(ca).cambiarDirector(codSuc, codDir);
                } catch (Exception e) { 
                    System.err.println("Error Director en Sucursal " + s[0]);
                }
            }

            // =========================================================================
            // 5. VINOS (FRAGMENTADOS - SOLO EN SU ORIGEN)
            // =========================================================================
            System.out.println("5. Insertando Vinos (Sin Replicar)...");
            Object[][] vinos = {
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
                    // Fragmentación: Insertar SOLO en el nodo de la CA
                    getOpsPorCCAA(ca).altaVino(
                        (int)v[0], (String)v[1], (int)v[2], (String)v[3], 
                        (double)v[4], (String)v[5], ca, (int)v[7], (int)v[8]
                    );
                } catch (Exception e) { System.err.println("Error Vino " + v[0]); }
            }

            // =========================================================================
            // 6. CLIENTES (FRAGMENTADOS - POR CCAA)
            // =========================================================================
            System.out.println("6. Insertando Clientes...");
            Object[][] clientes = {
                {1, "26262626A", "Hipercor", "Jaén", "A", "Andalucía"},
                {2, "272727278", "Restaurante Cacereño", "Cáceres", "C", "Extremadura"},
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
                } catch (Exception e) { System.err.println("Error Cliente " + c[0]); }
            }

            // =========================================================================
            // 7. SUMINISTROS (EN LA DELEGACIÓN DEL CLIENTE/SUCURSAL)
            // =========================================================================
            System.out.println("7. Gestionando Suministros...");
            Object[][] suministros = {
                {1, 1, 4, "2025-06-12", 100, "Andalucía"},
                {1, 2, 5, "2025-07-11", 150, "Andalucía"},
                {1, 3, 14, "2025-07-15", 200, "Andalucía"},
                {2, 2, 2, "2025-04-03", 20, "Andalucía"}, 
                {3, 10, 3, "2025-02-21", 100, "Galicia"},
                {3, 11, 13, "2025-10-03", 200, "Galicia"},
                {5, 7, 16, "2025-08-14", 50, "Cataluña"}, 
                {7, 4, 1, "2025-02-15", 80, "Madrid"},
                {7, 5, 7, "2025-04-17", 50, "Castilla-León"}
            };
            for (Object[] s : suministros) {
                try {
                    String caDest = (String)s[5];
                    getOpsPorCCAA(caDest).gestionarSuministro(
                        (int)s[0], (int)s[1], (int)s[2], Date.valueOf((String)s[3]), (int)s[4]
                    );
                } catch (Exception e) { 
                    System.err.println("Error Suministro Cli " + s[0] + ": " + e.getMessage()); 
                }
            }

            //=========================================================================
            // 8. PEDIDOS (DIRECTOS AL ORIGEN)
            // =========================================================================
            System.out.println("8. Realizando Pedidos Directos...");
            Object[][] pedidos = {
                // 1. Sevilla (1) quiere vino de Vega Sicilia (1).
                {1, 4, 4, "2025-06-13", 100}, 
                
                // 2. Granada (2) quiere vino de Tablas de Daimiel (14).
                {2, 7, 5, "2025-07-12", 150},
                
                // 3. Cádiz (3) quiere vino de Santa María (15).
                {4, 1, 10, "2025-06-22", 70}, 
                
                // 4. Madrid (4) pide vino de Uva Dorada (18).
                {7, 3, 21, "2025-09-18", 200}, 
                
                // 5. Burgos (5) pide vino de Altamira (20).
                {10, 4, 3, "2025-02-22", 100}  
            };
            
            for (Object[] p : pedidos) {
                try {
                    int sucPide = (int)p[0];
                    String caPide = mapaSucursalCCAA.get(sucPide);
                    
                    // Ejecutamos la petición desde la delegación solicitante
                    getOpsPorCCAA(caPide).altaPedidoSucursal(
                        (int)p[0], (int)p[1], (int)p[2], Date.valueOf((String)p[3]), (int)p[4]
                    );
                    System.out.println("   -> Pedido OK: Suc " + p[0] + " pide a Suc " + p[1]);
                } catch (Exception e) { 
                    System.err.println("Error Pedido Suc " + p[0] + ": " + e.getMessage()); 
                }
            }
        } catch (Exception e) {
            System.err.println("Error general en carga de datos: " + e.getMessage());
        }
    }

    // --- MÉTODO AUXILIAR PARA DETERMINAR DELEGACIÓN ---
    private static OperacionesBD getOpsPorCCAA(String ccaa) {
        if (ccaa == null) return opsMadrid;

        switch (ccaa) {
            case "Castilla-León": case "Castilla-La Mancha": case "Aragón": case "Madrid": case "La Rioja":
                return opsMadrid;

            case "Cataluña": case "Baleares": case "País Valenciano": case "Murcia":
                return opsBcn;

            case "Galicia": case "Asturias": case "Cantabria": case "País Vasco": case "Navarra":
                return opsCoruna;

            case "Andalucía": case "Extremadura": case "Canarias": case "Ceuta": case "Melilla":
                return opsSur;

            default:
                System.err.println("AVISO: CCAA desconocida (" + ccaa + "). Usando Madrid.");
                return opsMadrid;
        }
    }
}