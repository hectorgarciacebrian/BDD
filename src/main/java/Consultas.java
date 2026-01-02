import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class Consultas {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int opcion = -1;

        while (opcion != 0) {
            System.out.println("\n=================================================");
            System.out.println("            MENU DE CONSULTAS SQL");
            System.out.println("=================================================");
            System.out.println("1. Listar clientes (Andalucia/CLM) con vino 'Tablas de Daimiel'");
            System.out.println("2. Consultar producción suministrada por Productor (Baleares/Extrem/PV)");
            System.out.println("3. Consultar suministros de una Sucursal (Rioja/Albariño)");
            System.out.println("0. Salir");
            System.out.print("Seleccione una opción: ");

            try {
                opcion = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
                opcion = -1;
            }

            switch (opcion) {
                case 1:
                    consultaUno();
                    break;
                case 2:
                    System.out.print("Introduce el Código del Productor: ");
                    try {
                        int codProd = Integer.parseInt(scanner.nextLine());
                        consultaDos(codProd);
                    } catch (NumberFormatException e) {
                        System.out.println("Error: El código debe ser un número.");
                    }
                    break;
                case 3:
                    System.out.print("Introduce el Código de la Sucursal: ");
                    try {
                        int codSuc = Integer.parseInt(scanner.nextLine());
                        consultaTres(codSuc);
                    } catch (NumberFormatException e) {
                        System.out.println("Error: El código debe ser un número.");
                    }
                    break;
                case 0:
                    System.out.println("Saliendo...");
                    break;
                default:
                    System.out.println("Opción no válida.");
            }
        }
        scanner.close();
    }

    // -------------------------------------------------------------------------
    // CONSULTA 1
    // "Listar los clientes (nombre y dirección) de Andalucía o Castilla-La Mancha 
    // y las sucursales (nombre y ciudad), a los que se le ha suministrado vino 
    // de marca “Tablas de Daimiel” entre el 1 de Enero de 2025 y el 1 de septiembre de 2025".
    // -------------------------------------------------------------------------
    private static void consultaUno() {
        String sql = """
            SELECT c.nombre_c, c.direccion_c, s.nombre AS nombre_sucursal, s.ciudad
            FROM Pide p
            JOIN Cliente c  ON p.cod_cliente = c.cod_c
            JOIN Sucursal s ON p.cod_sucursal = s.cod_sucursal
            JOIN Vino v     ON p.cod_vino = v.cod_vino
            WHERE v.nombre_v = 'Tablas de Daimiel'
              AND (c.c_autonoma = 'Andalucía' OR c.c_autonoma = 'Castilla-La Mancha')
              AND p.fecha_pide >= TO_DATE('2025-01-01', 'YYYY-MM-DD')
              AND p.fecha_pide <= TO_DATE('2025-09-01', 'YYYY-MM-DD')
        """;

        System.out.println("\n--- RESULTADOS CONSULTA 1 ---");
        ejecutarConsulta(sql);
    }

    // -------------------------------------------------------------------------
    // CONSULTA 2
    // Dado por teclado el código de un productor: “Listar la marca, el año de cosecha 
    // de cada uno de los vinos producidos por él y la cantidad total suministrada 
    // de cada uno de ellos a clientes de las comunidades autónomas de 
    // Baleares, Extremadura o País Valenciano”.
    // -------------------------------------------------------------------------
    private static void consultaDos(int codProductor) {
        String sql = """
            SELECT v.nombre_v, v.anio, SUM(p.cantidad) AS total_suministrado
            FROM Vino v
            JOIN Pide p    ON v.cod_vino = p.cod_vino
            JOIN Cliente c ON p.cod_cliente = c.cod_c
            WHERE v.productor = ?
              AND c.c_autonoma IN ('Baleares', 'Extremadura', 'País Valenciano')
            GROUP BY v.nombre_v, v.anio
            ORDER BY v.nombre_v
        """;

        System.out.println("\n--- RESULTADOS CONSULTA 2 (Productor " + codProductor + ") ---");
        ejecutarConsulta(sql, codProductor);
    }

    // -------------------------------------------------------------------------
    // CONSULTA 3
    // Dado por teclado el código de una sucursal: “Listar el nombre de cada uno de 
    // sus clientes, su tipo y la cantidad total vino de Rioja o Albariño que se le 
    // ha suministrado a cada uno de ellos (solamente aparecerán aquellos clientes 
    // a los que se les ha suministrado vinos con esta DO)”.
    // -------------------------------------------------------------------------
    private static void consultaTres(int codSucursal) {
        String sql = """
            SELECT c.nombre_c, c.tipo_c, SUM(p.cantidad) AS total_vino
            FROM Pide p
            JOIN Cliente c ON p.cod_cliente = c.cod_c
            JOIN Vino v    ON p.cod_vino = v.cod_vino
            WHERE p.cod_sucursal = ?
              AND (v.denominacion = 'Rioja' OR v.denominacion = 'Albariño')
            GROUP BY c.nombre_c, c.tipo_c
            ORDER BY c.nombre_c
        """;

        System.out.println("\n--- RESULTADOS CONSULTA 3 (Sucursal " + codSucursal + ") ---");
        ejecutarConsulta(sql, codSucursal);
    }

    // Metodo genérico para ejecutar una consulta SQL con parámetros opcionales
    private static void ejecutarConsulta(String sql, Object... params) {
        DatabaseManager db = DatabaseManager.getInstance();
        
        try (Connection conn = db.getConnection(DatabaseManager.Delegacion.MADRID);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // Asignar parámetros (?)
            for (int i = 0; i < params.length; i++) {
                if (params[i] instanceof Integer) {
                    ps.setInt(i + 1, (Integer) params[i]);
                } else {
                    ps.setObject(i + 1, params[i]);
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                boolean hayDatos = false;
                int columnas = rs.getMetaData().getColumnCount();

                // Cabecera simple
                for (int i = 1; i <= columnas; i++) {
                    System.out.printf("%-25s", rs.getMetaData().getColumnLabel(i));
                }
                System.out.println("\n" + "-".repeat(columnas * 25));

                // Filas
                while (rs.next()) {
                    hayDatos = true;
                    for (int i = 1; i <= columnas; i++) {
                        String valor = rs.getString(i);
                        if (valor == null) valor = "NULL";
                        // Recortar si es muy largo para que la tabla no se rompa visualmente
                        if (valor.length() > 22) valor = valor.substring(0, 22) + "..";
                        System.out.printf("%-25s", valor);
                    }
                    System.out.println();
                }

                if (!hayDatos) {
                    System.out.println("(No se encontraron resultados para esta consulta)");
                }
            }

        } catch (SQLException e) {
            System.err.println("ERROR SQL: " + e.getMessage());
        }
    }
}