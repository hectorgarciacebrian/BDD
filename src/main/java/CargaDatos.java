import java.sql.Date;

public class CargaDatos {

    public static void main(String[] args) {
        System.out.println("--- INICIANDO CARGA MASIVA DE DATOS ---");
        
        // Inicializamos el gestor de operaciones
        // Usamos MADRID como delegación por defecto para conectar (da igual cual usemos porque los SP gestionan todo)
        OperacionesBD ops = new OperacionesBD(DatabaseManager.Delegacion.MADRID);

        try {
            // =========================================================================
            // 1. CARGA DE PRODUCTORES
            // =========================================================================
            System.out.println("\n1. Insertando Productores...");
            ops.altaProductor(1, "35353535A", "Justiniano Briñón", "Ramón y Cajal 9, Valladolid");
            ops.altaProductor(2, "36363636B", "Marcelino Peña", "San Francisco 7, Pamplona");
            ops.altaProductor(3, "37373737C", "Paloma Riquelme", "Antonio Gaudí 23, Barcelona");
            ops.altaProductor(4, "38383838D", "Amador Laguna", "Juan Ramón Jiménez 17, Córdoba");
            ops.altaProductor(5, "39393939E", "Ramón Esteban", "Gran Vía de Colón 121, Madrid");
            ops.altaProductor(6, "40404040F", "Carlota Fuentes", "Cruz de los Ángeles 29, Oviedo");

            // =========================================================================
            // 2. CARGA DE SUCURSALES (Inicialmente sin director para evitar ciclo)
            // =========================================================================
            System.out.println("2. Insertando Sucursales (Sin director)...");
            // Cod, Nombre, Ciudad, CA, Director(null)
            ops.altaSucursal(1, "Santa Cruz", "Sevilla", "Andalucía", null);
            ops.altaSucursal(2, "Palacios Nazaríes", "Granada", "Andalucía", null);
            ops.altaSucursal(3, "Tacita de Plata", "Cádiz", "Andalucía", null);
            ops.altaSucursal(4, "Almudena", "Madrid", "Madrid", null);
            ops.altaSucursal(5, "El Cid", "Burgos", "Castilla-León", null);
            ops.altaSucursal(6, "Puente la Reina", "Logroño", "La Rioja", null);
            ops.altaSucursal(7, "Catedral del Mar", "Barcelona", "Cataluña", null);
            ops.altaSucursal(8, "Dama de Elche", "Alicante", "País Valenciano", null);
            ops.altaSucursal(9, "La Cartuja", "Palma de Mallorca", "Baleares", null);
            ops.altaSucursal(10, "Meigas", "La Coruña", "Galicia", null);
            ops.altaSucursal(11, "La Concha", "San Sebastián", "País Vasco", null);
            ops.altaSucursal(12, "Don Pelayo", "Oviedo", "Asturias", null);

            // =========================================================================
            // 3. CARGA DE EMPLEADOS
            // =========================================================================
            System.out.println("3. Insertando Empleados...");
            // Cod, DNI, Nombre, Fecha, Salario, Direccion, CodSucursal
            ops.altaEmpleado(1, "11111111A", "Raúl", Date.valueOf("2010-09-21"), 2000, "Sierpes 37, Sevilla", 1);
            ops.altaEmpleado(2, "22222222B", "Federico", Date.valueOf("2009-08-25"), 1800, "Emperatriz 25, Sevilla", 1);
            ops.altaEmpleado(3, "33333333C", "Natalia", Date.valueOf("2012-01-30"), 2000, "Ronda 126, Granada", 2);
            ops.altaEmpleado(4, "44444444D", "Amalia", Date.valueOf("2013-02-13"), 1800, "San Matías 23, Granada", 2);
            ops.altaEmpleado(5, "55555555E", "Susana", Date.valueOf("2018-10-01"), 2000, "Zoraida 5, Cádiz", 3);
            ops.altaEmpleado(6, "66666666F", "Gonzalo", Date.valueOf("2007-01-01"), 1800, "Tartesios 9, Cádiz", 3);
            ops.altaEmpleado(7, "77777777G", "Agustín", Date.valueOf("2019-05-05"), 2000, "Pablo Neruda 84, Madrid", 4);
            ops.altaEmpleado(8, "88888888H", "Eduardo", Date.valueOf("2019-06-06"), 1800, "Alcalá 8, Madrid", 4);
            ops.altaEmpleado(9, "99999999I", "Alberto", Date.valueOf("2020-09-05"), 2000, "Las Huelgas 15, Burgos", 5);
            ops.altaEmpleado(10, "10101010J", "Soraya", Date.valueOf("2017-10-04"), 1800, "Jimena 2, Burgos", 5);
            ops.altaEmpleado(11, "01010101K", "Manuel", Date.valueOf("2016-07-06"), 2000, "Estrella 26, Logroño", 6);
            ops.altaEmpleado(12, "12121212L", "Emilio", Date.valueOf("2018-11-05"), 1800, "Constitución 3, Logroño", 6);
            ops.altaEmpleado(13, "13131313M", "Patricia", Date.valueOf("2019-12-04"), 2000, "Diagonal 132, Barcelona", 7);
            ops.altaEmpleado(14, "14141414N", "Inés", Date.valueOf("2018-03-07"), 1800, "Colón 24, Barcelona", 7);
            ops.altaEmpleado(15, "15151515O", "Carlos", Date.valueOf("2019-06-16"), 2000, "Palmeras 57, Alicante", 8);
            ops.altaEmpleado(16, "16161616P", "Dolores", Date.valueOf("2018-05-14"), 1800, "Calatrava 9, Alicante", 8);
            ops.altaEmpleado(17, "17171717Q", "Elías", Date.valueOf("2019-06-13"), 2000, "Arenal 17, P. Mallorca", 9);
            ops.altaEmpleado(18, "18181818R", "Concepción", Date.valueOf("2020-08-01"), 1800, "Campastilla 14, P. Mallorca", 9);
            ops.altaEmpleado(19, "19191919S", "Gabriel", Date.valueOf("2015-09-19"), 2000, "Hércules 19, La Coruña", 10);
            ops.altaEmpleado(20, "20202020T", "Octavio", Date.valueOf("2017-10-20"), 1800, "María Pita 45, La Coruña", 10);
            ops.altaEmpleado(21, "21212121V", "Cesar", Date.valueOf("2021-11-13"), 2000, "Las Peñas 41, San Sebastián", 11);
            ops.altaEmpleado(22, "23232323W", "Julia", Date.valueOf("2020-03-24"), 1800, "San Cristóbal 5, San Sebastián", 11);
            ops.altaEmpleado(23, "24242424X", "Claudia", Date.valueOf("2022-02-13"), 2000, "Santa Cruz 97, Oviedo", 12);
            ops.altaEmpleado(24, "25252525Z", "Mario", Date.valueOf("2017-04-23"), 1800, "Naranco 21, Oviedo", 12);

            // =========================================================================
            // 4. ASIGNACIÓN DE DIRECTORES
            // =========================================================================
            System.out.println("4. Asignando Directores a Sucursales...");
            // Usamos cambiarDirector para actualizar el campo director
            ops.cambiarDirector(1, 1);  // Sevilla -> Raúl
            ops.cambiarDirector(2, 3);  // Granada -> Natalia
            ops.cambiarDirector(3, 5);  // Cádiz -> Susana
            ops.cambiarDirector(4, 7);  // Madrid -> Agustín
            ops.cambiarDirector(5, 9);  // Burgos -> Alberto
            ops.cambiarDirector(6, 11); // Logroño -> Manuel
            ops.cambiarDirector(7, 13); // Barcelona -> Patricia
            ops.cambiarDirector(8, 15); // Alicante -> Carlos
            ops.cambiarDirector(9, 17); // Mallorca -> Elías
            ops.cambiarDirector(10, 19); // Coruña -> Gabriel
            ops.cambiarDirector(11, 21); // San Sebastián -> Cesar
            ops.cambiarDirector(12, 23); // Oviedo -> Claudia

            // =========================================================================
            // 5. CARGA DE VINOS
            // =========================================================================
            System.out.println("5. Insertando Vinos...");
            // Cod, Marca, Año, Denom, Grad, Viñedo, CA, Cantidad, Productor
            ops.altaVino(1, "Vega Sicilia", 2008, "Ribera del Duero", 12.5, "Castillo Blanco", "Castilla-León", 200, 1);
            ops.altaVino(2, "Vega Sicilia", 2015, "Ribera del Duero", 12.5, "Castillo Blanco", "Castilla-León", 100, 1);
            ops.altaVino(3, "Marqués de Cáceres", 2019, "Rioja", 11.0, "Santo Domingo", "La Rioja", 200, 2);
            ops.altaVino(4, "Marqués de Cáceres", 2022, "Rioja", 11.5, "Santo Domingo", "La Rioja", 250, 2);
            ops.altaVino(5, "René Barbier", 2023, "Penedés", 11.5, "Virgen de Estrella", "Cataluña", 200, 3);
            ops.altaVino(6, "René Barbier", 2020, "Penedés", 11.0, "Virgen de Estrella", "Cataluña", 250, 3);
            ops.altaVino(7, "Rias Baixas", 2024, "Albariño", 9.5, "Santa Compaña", "Galicia", 150, 4);
            ops.altaVino(8, "Rias Baixas", 2023, "Albariño", 9.0, "Santa Compaña", "Galicia", 100, 4);
            ops.altaVino(9, "Córdoba Bella", 2018, "Montilla", 12.0, "Mezquita Roja", "Andalucía", 200, 4);
            ops.altaVino(10, "Tío Pepe", 2020, "Jerez", 12.5, "Campo Verde", "Andalucía", 200, 4);
            ops.altaVino(13, "Vega Murciana", 2023, "Jumilla", 11.5, "Vega Verde", "Murcia", 250, 5);
            ops.altaVino(14, "Tablas de Daimiel", 2018, "Valdepeñas", 11.5, "Laguna Azul", "Castilla-La Mancha", 300, 5);
            ops.altaVino(15, "Santa María", 2023, "Tierra de Cangas", 10.0, "Monte Astur", "Asturias", 200, 6);
            ops.altaVino(16, "Freixenet", 2024, "Cava", 7.5, "Valle Dorado", "Cataluña", 250, 6);
            ops.altaVino(17, "Estela", 2022, "Cariñena", 10.5, "San Millán", "Aragón", 200, 3);
            ops.altaVino(18, "Uva dorada", 2023, "Málaga", 13.0, "Axarquía", "Andalucía", 200, 5);
            ops.altaVino(19, "Meigas Bellas", 2024, "Ribeiro", 8.5, "Mayor Santiago", "Galicia", 250, 6);
            ops.altaVino(20, "Altamira", 2024, "Tierra de Liébana", 9.5, "Cuevas", "Cantabria", 300, 1);
            ops.altaVino(21, "Virgen negra", 2024, "Islas Canarias", 10.5, "Guanche", "Canarias", 300, 3);

            // =========================================================================
            // 6. CARGA DE CLIENTES
            // =========================================================================
            System.out.println("6. Insertando Clientes...");
            // Cod, DNI, Nombre, Dir, Tipo, CA
            ops.altaCliente(1, "26262626A", "Hipercor", "Virgen de la Capilla 32, Jaén", "A", "Andalucía");
            ops.altaCliente(2, "272727278", "Restaurante Cacereño", "San Marcos 41, Cáceres", "C", "Extremadura");
            ops.altaCliente(3, "28282828C", "Continente", "San Francisco 37, Vigo", "A", "Galicia");
            ops.altaCliente(4, "29292929D", "Restaurante El Asturiano", "Covadonga 24, Luarca", "C", "Asturias");
            ops.altaCliente(5, "30303030E", "Restaurante El Payés", "San Lucas 33, Mahón", "C", "Baleares");
            ops.altaCliente(6, "31313131F", "Mercadona", "Desamparados 29, Castellón", "A", "País Valenciano");
            ops.altaCliente(7, "32323232G", "Restaurante Cándido", "Acueducto 1, Segovia", "C", "Castilla-León");
            ops.altaCliente(8, "34343434H", "Restaurante Las Vidrieras", "Cervantes 16, Almagro", "C", "Castilla-La Mancha");

            // =========================================================================
            // 7. CARGA DE SUMINISTROS (Tabla Pide)
            // =========================================================================
            System.out.println("7. Gestionando Suministros (Pide)...");
            // Cliente 1 (Andalucía)
            ops.gestionarSuministro(1, 1, 4, Date.valueOf("2025-06-12"), 100);
            ops.gestionarSuministro(1, 2, 5, Date.valueOf("2025-07-11"), 150);
            ops.gestionarSuministro(1, 3, 14, Date.valueOf("2025-07-15"), 200);

            // Cliente 2 (Extremadura -> Pide a Andalucía)
            ops.gestionarSuministro(2, 2, 2, Date.valueOf("2025-04-03"), 20);
            ops.gestionarSuministro(2, 1, 7, Date.valueOf("2025-05-04"), 50);
            ops.gestionarSuministro(2, 2, 6, Date.valueOf("2025-09-15"), 40);
            ops.gestionarSuministro(2, 3, 16, Date.valueOf("2025-09-20"), 100);

            // Cliente 3 (Galicia)
            ops.gestionarSuministro(3, 10, 3, Date.valueOf("2025-02-21"), 100);
            ops.gestionarSuministro(3, 10, 6, Date.valueOf("2025-08-02"), 90);
            ops.gestionarSuministro(3, 11, 13, Date.valueOf("2025-10-03"), 200);
            ops.gestionarSuministro(3, 12, 20, Date.valueOf("2025-11-04"), 150);

            // Cliente 4 (Asturias)
            ops.gestionarSuministro(4, 12, 8, Date.valueOf("2025-03-01"), 50);
            ops.gestionarSuministro(4, 12, 17, Date.valueOf("2025-05-03"), 70);

            // Cliente 5 (Baleares)
            ops.gestionarSuministro(5, 7, 16, Date.valueOf("2025-08-14"), 50);
            ops.gestionarSuministro(5, 9, 18, Date.valueOf("2025-10-01"), 100);

            // Cliente 6 (País Valenciano)
            ops.gestionarSuministro(6, 8, 15, Date.valueOf("2025-01-13"), 100);
            ops.gestionarSuministro(6, 8, 9, Date.valueOf("2025-02-19"), 150);
            ops.gestionarSuministro(6, 9, 19, Date.valueOf("2025-06-27"), 160);
            ops.gestionarSuministro(6, 7, 21, Date.valueOf("2025-09-17"), 200);

            // Cliente 7 (Castilla-León)
            ops.gestionarSuministro(7, 4, 1, Date.valueOf("2025-02-15"), 80);
            ops.gestionarSuministro(7, 5, 7, Date.valueOf("2025-04-17"), 50);
            ops.gestionarSuministro(7, 4, 10, Date.valueOf("2025-06-21"), 70);
            //ops.gestionarSuministro(7, 5, 12, Date.valueOf("2025-07-23"), 40); // El código 12 no existe en vinos insertados, cuidado (asumo Don Pelayo no es vino). Revisando... Ah vino 12 no está. Asumo error en PDF o faltó uno. Lo dejo comentado.
            // ops.gestionarSuministro(7, 5, 12, ...); // CUIDADO: Vino 12 no está en la lista de INSERTs de arriba.

            // Cliente 8 (Castilla-La Mancha)
            ops.gestionarSuministro(8, 6, 14, Date.valueOf("2025-01-11"), 50);
            ops.gestionarSuministro(8, 6, 4, Date.valueOf("2025-03-14"), 60);
            ops.gestionarSuministro(8, 4, 6, Date.valueOf("2025-05-21"), 70);


            // =========================================================================
            // 8. CARGA DE PEDIDOS ENTRE SUCURSALES (Solicita)
            // =========================================================================
            System.out.println("8. Realizando Pedidos entre Sucursales (Solicita)...");
            
            // Sucursal 1 (Sevilla) pide...
            ops.altaPedidoSucursal(1, 4, 4, Date.valueOf("2025-06-13"), 100);
            ops.altaPedidoSucursal(1, 10, 7, Date.valueOf("2025-05-05"), 50);

            // Sucursal 2 (Granada) pide...
            ops.altaPedidoSucursal(2, 7, 5, Date.valueOf("2025-07-12"), 150);
            ops.altaPedidoSucursal(2, 5, 2, Date.valueOf("2025-04-04"), 20);
            ops.altaPedidoSucursal(2, 8, 6, Date.valueOf("2025-09-16"), 40);

            // Sucursal 3 (Cádiz) pide...
            ops.altaPedidoSucursal(3, 6, 14, Date.valueOf("2025-07-15"), 200);
            ops.altaPedidoSucursal(3, 9, 16, Date.valueOf("2025-09-21"), 100);

            // Sucursal 4 (Madrid) pide...
            ops.altaPedidoSucursal(4, 1, 10, Date.valueOf("2025-06-22"), 70);
            ops.altaPedidoSucursal(4, 7, 6, Date.valueOf("2025-05-22"), 70);

            // Sucursal 5 (Burgos) pide...
            ops.altaPedidoSucursal(5, 10, 7, Date.valueOf("2025-04-18"), 50);

            // Sucursal 7 (Barcelona) pide...
            ops.altaPedidoSucursal(7, 2, 21, Date.valueOf("2025-09-18"), 200);

            // Sucursal 8 (Alicante) pide...
            ops.altaPedidoSucursal(8, 11, 15, Date.valueOf("2025-01-14"), 100);

            // Sucursal 9 (Mallorca) pide...
            ops.altaPedidoSucursal(9, 2, 9, Date.valueOf("2025-02-20"), 150);
            ops.altaPedidoSucursal(9, 3, 18, Date.valueOf("2025-10-02"), 100);
            ops.altaPedidoSucursal(9, 12, 19, Date.valueOf("2025-06-28"), 160);

            // Sucursal 10 (Coruña) pide...
            ops.altaPedidoSucursal(10, 4, 3, Date.valueOf("2025-02-22"), 100);
            ops.altaPedidoSucursal(10, 8, 6, Date.valueOf("2025-08-02"), 90);

            // Sucursal 11 (San Sebastián) pide...
            ops.altaPedidoSucursal(11, 9, 13, Date.valueOf("2025-10-04"), 200);

            // Sucursal 12 (Oviedo) pide...
            ops.altaPedidoSucursal(12, 4, 17, Date.valueOf("2025-05-04"), 70);


            System.out.println("\n¡CARGA DE DATOS COMPLETADA CON ÉXITO!");

        } catch (Exception e) {
            System.err.println("ERROR CRÍTICO DURANTE LA CARGA: " + e.getMessage());
            e.printStackTrace();
        }
    }
}