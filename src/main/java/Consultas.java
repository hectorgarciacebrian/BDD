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
            System.out.println("            MENU DE CONSULTAS SQL (GLOBALES)");
            System.out.println("=================================================");
            System.out.println("1. Clientes (Andalucía/CLM) con vino 'Tablas de Daimiel' (Ene-Sep 2025)");
            System.out.println("2. Producción por Productor a Baleares/Extremadura/PV");
            System.out.println("3. Ventas de Sucursal (Rioja/Albariño) por cliente");
            System.out.println("0. Salir");
            System.out.print("Seleccione una opción: ");

            try {
                String input = scanner.nextLine();
                if(input.isEmpty()) continue;
                opcion = Integer.parseInt(input);
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
                    } catch(NumberFormatException e) { System.err.println("Código inválido."); }
                    break;
                case 3:
                    System.out.print("Introduce el Código de la Sucursal: ");
                    try {
                        int codSuc = Integer.parseInt(scanner.nextLine());
                        consultaTres(codSuc);
                    } catch(NumberFormatException e) { System.err.println("Código inválido."); }
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

    // --- HELPER PARA IMPRIMIR RESULTADOS ---
    private static void imprimirResultSet(ResultSet rs) throws SQLException {
        boolean hayDatos = false;
        int columnas = rs.getMetaData().getColumnCount();

        // Cabecera
        System.out.println();
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
                if (valor.length() > 22) valor = valor.substring(0, 22) + "..";
                System.out.printf("%-25s", valor);
            }
            System.out.println();
        }

        if (!hayDatos) {
            System.out.println("(No se encontraron resultados para esta consulta)");
        }
    }

    // =========================================================================
    // CONSULTA 1: Clientes Andalucía/CLM, Vino 'Tablas de Daimiel', Fechas
    // =========================================================================
    private static void consultaUno() {
        DatabaseManager db = DatabaseManager.getInstance();
        
        // Usamos MADRID como punto de entrada para consultar las VISTAS GLOBALES
        try (Connection conn = db.getConnection(DatabaseManager.Delegacion.MADRID)) {
            
            String sql = """
                SELECT DISTINCT C.nombre AS Cliente, C.direccion, S.nombre AS Sucursal, S.ciudad
                FROM Vista_Clientes C
                JOIN Vista_Suministros_Clientes P ON C.cod_c = P.cod_cliente
                JOIN Vista_Sucursales S ON P.cod_sucursal = S.cod_sucursal
                JOIN Vista_Vinos V ON P.cod_vino = V.cod_vino
                WHERE V.nombre = 'Tablas de Daimiel'
                  AND C.c_autonoma IN ('Andalucía', 'Castilla-La Mancha')
                  AND P.fecha_pide >= TO_DATE('01/01/2025', 'DD/MM/YYYY')
                  AND P.fecha_pide <= TO_DATE('01/09/2025', 'DD/MM/YYYY')
            """;

            System.out.println("Ejecutando Consulta 1...");
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                imprimirResultSet(rs);
            }

        } catch (SQLException e) {
            System.err.println("Error SQL: " + e.getMessage());
        }
    }

    // =========================================================================
    // CONSULTA 2: Productor -> Vinos suministrados a Baleares/Extremadura/PV
    // =========================================================================
    private static void consultaDos(int codProductor) {
        DatabaseManager db = DatabaseManager.getInstance();

        try (Connection conn = db.getConnection(DatabaseManager.Delegacion.MADRID)) {

            String sql = """
                SELECT V.nombre AS Marca, V.anio AS Cosecha, SUM(P.cantidad) AS Total_Suministrado
                FROM Vista_Vinos V
                JOIN Vista_Suministros_Clientes P ON V.cod_vino = P.cod_vino
                JOIN Vista_Clientes C ON P.cod_cliente = C.cod_c
                WHERE V.productor = ?
                  AND C.c_autonoma IN ('Baleares', 'Extremadura', 'País Valenciano')
                GROUP BY V.nombre, V.anio
                ORDER BY Total_Suministrado DESC
            """;

            System.out.println("Ejecutando Consulta 2 para Productor ID: " + codProductor);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, codProductor);
                try (ResultSet rs = ps.executeQuery()) {
                    imprimirResultSet(rs);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error SQL: " + e.getMessage());
        }
    }

    // =========================================================================
    // CONSULTA 3: Sucursal -> Clientes que compraron Rioja o Albariño
    // =========================================================================
    private static void consultaTres(int codSucursal) {
        DatabaseManager db = DatabaseManager.getInstance();

        try (Connection conn = db.getConnection(DatabaseManager.Delegacion.MADRID)) {

            String sql = """
                SELECT C.nombre AS Cliente, C.tipo_c AS Tipo, SUM(P.cantidad) AS Cantidad_Total
                FROM Vista_Clientes C
                JOIN Vista_Suministros_Clientes P ON C.cod_c = P.cod_cliente
                JOIN Vista_Vinos V ON P.cod_vino = V.cod_vino
                WHERE P.cod_sucursal = ?
                  AND V.denominacion IN ('Rioja', 'Albariño')
                GROUP BY C.nombre, C.tipo_c
                ORDER BY C.nombre
            """;

            System.out.println("Ejecutando Consulta 3 para Sucursal ID: " + codSucursal);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, codSucursal);
                try (ResultSet rs = ps.executeQuery()) {
                    imprimirResultSet(rs);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error SQL: " + e.getMessage());
        }
    }
}