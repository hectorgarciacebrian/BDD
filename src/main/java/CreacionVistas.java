import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreacionVistas {

    public static void main(String[] args) {
        DatabaseManager db = DatabaseManager.getInstance();
        System.out.println("--- CREANDO VISTAS (UNA POR TABLA) ---");

        // Array con las sentencias SQL para crear una vista por cada tabla
        String[] vistas = {
            
            // 1. VISTA SUCURSALES
            """
            CREATE OR REPLACE VIEW Vista_Sucursales AS
            SELECT cod_sucursal, nombre, ciudad, c_autonoma, director
            FROM Sucursal
            """,

            // 2. VISTA PRODUCTORES
            """
            CREATE OR REPLACE VIEW Vista_Productores AS
            SELECT cod_p, dni_p, nombre_p, direccion_p
            FROM Productor
            """,

            // 3. VISTA VINOS
            // Muestra los datos del vino. (Podríamos hacer JOIN con Productor para ver el nombre, pero mantenemos la estructura base)
            """
            CREATE OR REPLACE VIEW Vista_Vinos AS
            SELECT cod_vino, nombre_v AS Marca, anio, denominacion, graduacion, vinedo, c_autonoma, stock, productor
            FROM Vino
            """,

            // 4. VISTA EMPLEADOS
            """
            CREATE OR REPLACE VIEW Vista_Empleados AS
            SELECT cod_e, dni_e, nombre_e, fecha_comp, salario, sucursal_dest
            FROM Empleado
            """,

            // 5. VISTA CLIENTES
            """
            CREATE OR REPLACE VIEW Vista_Clientes AS
            SELECT cod_c, dni_c, nombre_c, direccion_c, tipo_c, c_autonoma
            FROM Cliente
            """,

            // 6. VISTA SUMINISTROS (Relación Pide: Cliente -> Sucursal)
            """
            CREATE OR REPLACE VIEW Vista_Suministros_Clientes AS
            SELECT cod_cliente, cod_sucursal, cod_vino, fecha_pide, cantidad
            FROM Pide
            """,

            // 7. VISTA PEDIDOS INTERNOS (Relación Solicita: Sucursal -> Sucursal)
            """
            CREATE OR REPLACE VIEW Vista_Pedidos_Entre_Sucursales AS
            SELECT cod_sucursal AS Sucursal_Solicitante, 
                   cod_sucursal_prov AS Sucursal_Proveedora, 
                   cod_tipo_vino AS Vino, 
                   fecha_sol, 
                   cantidad
            FROM Solicita
            """,

            // 8. VISTA CATALOGO (Relación Suministra: Qué vinos tiene cada sucursal)
            """
            CREATE OR REPLACE VIEW Vista_Catalogo_Distribucion AS
            SELECT cod_sucursal, cod_vino, fecha_su
            FROM Suministra
            """
        };

        // Ejecución de las vistas en cada nodo/delegación
        for (DatabaseManager.Delegacion d : DatabaseManager.Delegacion.values()) {
            System.out.println("\nProcesando nodo: " + d);
            try (Connection conn = db.getConnection(d);
                 Statement stmt = conn.createStatement()) {

                System.out.print("Generando vistas... ");
                for (String sql : vistas) {
                    try {
                        stmt.executeUpdate(sql);
                    } catch (SQLException e) {
                        System.err.println("\nError creando vista: " + e.getMessage());
                    }
                }
                System.out.println("OK.");

            } catch (SQLException e) {
                System.err.println("Error de conexión: " + e.getMessage());
            }
        }
        System.out.println("\nTODAS LAS VISTAS CREADAS CORRECTAMENTE.");
    }
}