import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreacionVistas {

    public static void main(String[] args) {
        DatabaseManager db = DatabaseManager.getInstance();
        System.out.println("--- CREANDO VISTAS GLOBALES (ESQUEMAS: cerveza1..cerveza4) ---");

        // Recorremos cada nodo para conectarnos y crear las vistas en su esquema
        for (DatabaseManager.Delegacion nodoActual : DatabaseManager.Delegacion.values()) {
            System.out.println("\nProcesando nodo: " + nodoActual);

            try (Connection conn = db.getConnection(nodoActual);
                 Statement stmt = conn.createStatement()) {

                // Definimos las vistas usando nombres de esquema explícitos (cervezaX.Tabla)
                String[] vistas = {
                    // 1. VISTA SUCURSALES
                    "CREATE OR REPLACE VIEW Vista_Sucursales AS " +
                    generarUnionEsquemas("cod_sucursal, nombre, ciudad, c_autonoma, director", "Sucursal"),

                    // 2. VISTA PRODUCTORES
                    "CREATE OR REPLACE VIEW Vista_Productores AS " +
                    generarUnionEsquemas("cod_p, dni_p, nombre_p, direccion_p", "Productor"),

                    // 3. VISTA VINOS
                    "CREATE OR REPLACE VIEW Vista_Vinos AS " +
                    generarUnionEsquemas("cod_vino, nombre_v AS Marca, anio, denominacion, graduacion, vinedo, c_autonoma, stock, productor", "Vino"),

                    // 4. VISTA EMPLEADOS
                    "CREATE OR REPLACE VIEW Vista_Empleados AS " +
                    generarUnionEsquemas("cod_e, dni_e, nombre_e, fecha_comp, salario, sucursal_dest", "Empleado"),

                    // 5. VISTA CLIENTES
                    "CREATE OR REPLACE VIEW Vista_Clientes AS " +
                    generarUnionEsquemas("cod_c, dni_c, nombre_c, direccion_c, tipo_c, c_autonoma", "Cliente"),

                    // 6. VISTA SUMINISTROS (Pide)
                    "CREATE OR REPLACE VIEW Vista_Suministros_Clientes AS " +
                    generarUnionEsquemas("cod_cliente, cod_sucursal, cod_vino, fecha_pide, cantidad", "Pide"),

                    // 7. VISTA PEDIDOS INTERNOS (Solicita)
                    "CREATE OR REPLACE VIEW Vista_Pedidos_Entre_Sucursales AS " +
                    generarUnionEsquemas("cod_sucursal AS Suc_Sol, cod_sucursal_prov AS Suc_Prov, cod_tipo_vino AS Vino, fecha_sol, cantidad", "Solicita"),

                    // 8. VISTA CATALOGO (Suministra)
                    "CREATE OR REPLACE VIEW Vista_Catalogo_Distribucion AS " +
                    generarUnionEsquemas("cod_sucursal, cod_vino, fecha_su", "Suministra")
                };

                // Ejecutamos la creación de vistas
                System.out.print("   -> Generando vistas... ");
                for (String sql : vistas) {
                    try {
                        stmt.executeUpdate(sql);
                    } catch (SQLException e) {
                        System.err.println("\n      Error SQL: " + e.getMessage());
                    }
                }
                System.out.println("OK.");

            } catch (SQLException e) {
                System.err.println("Error de conexión: " + e.getMessage());
            }
        }
        System.out.println("\nPROCESO TERMINADO.");
    }

    /**
     * Genera la consulta UNION ALL usando prefijos de esquema (cerveza1., cerveza2., etc.)
     * Ejemplo salida: SELECT ... FROM cerveza1.Sucursal UNION ALL SELECT ... FROM cerveza2.Sucursal ...
     */
    private static String generarUnionEsquemas(String columnas, String tabla) {
        StringBuilder sb = new StringBuilder();
        
        // Iteramos por las 4 delegaciones para construir la query global
        DatabaseManager.Delegacion[] todos = DatabaseManager.Delegacion.values();
        
        for (int i = 0; i < todos.length; i++) {
            DatabaseManager.Delegacion d = todos[i];
            
            // Obtenemos el nombre del esquema (cerveza1, cerveza2...)
            String esquema = getEsquema(d);
            
            sb.append("SELECT ").append(columnas).append(" FROM ");
            
            // Construimos referencia: cervezaX.Tabla
            sb.append(esquema).append(".").append(tabla);

            // Añadimos UNION ALL si no es el último
            if (i < todos.length - 1) {
                sb.append(" UNION ALL ");
            }
        }
        return sb.toString();
    }

    // Mapea la Delegación al nombre del Esquema/Usuario de la BD
    private static String getEsquema(DatabaseManager.Delegacion d) {
        switch (d) {
            case MADRID:    return "cerveza1";
            case BARCELONA: return "cerveza2";
            case CORUNA:    return "cerveza3";
            case SEVILLA:   return "cerveza4";
            default:        return "cerveza1";
        }
    }
}