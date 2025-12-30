import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class RestriccionesTriggers{
    public static void main(String[] args) {
        DatabaseManager db = DatabaseManager.getInstance();

        String[] cheks = {
            // Restricciones 8 y 14 
            "ALTER TABLE Cliente ADD CONSTRAINT CK_Tipo_Cliente CHECK (tipo_c IN ('A', 'B', 'C'))",
            "ALTER TABLE Vino ADD CONSTRAINT CK_Stock_Logico CHECK (stock >= 0 and stock <= c_producida)",
        };

        //Restricciones 1-5, 7, 11-13: Ya están cubiertas por las claves primarias (PK), foráneas (FK) y NOT NULL creadas en CreacionTablas.java.
        // Triggers
        // 6. El salario no puede disminuirse
        String triggerSalario = """
            CREATE OR REPLACE TRIGGER trg_Salario_NoDisminuye
            BEFORE UPDATE OF salario ON Empleado
            FOR EACH ROW
            BEGIN
                IF :NEW.salario < :OLD.salario THEN
                    RAISE_APPLICATION_ERROR(-20001, 'Error: El salario no puede disminuirse.');
                END IF;
            END;
            """;
        // 9. Cliente solo pide a sucursal de su delegacion
        String triggerClienteDelegacion = """
            CREATE OR REPLACE TRIGGER trg_Cliente_Pide_Delegacion
            BEFORE INSERT OR UPDATE ON Pide
            FOR EACH ROW
            DECLARE
                v_c_autonoma_cliente VARCHAR2(50);
                v_c_autonoma_sucursal VARCHAR2(50);
            BEGIN
                SELECT c_autonoma INTO v_c_autonoma_cliente FROM Cliente WHERE cod_c = :NEW.cod_cliente;
                SELECT c_autonoma INTO v_c_autonoma_sucursal FROM Sucursal WHERE cod_sucursal = :NEW.cod_sucursal;
                
                IF v_c_autonoma_cliente != v_c_autonoma_sucursal THEN
                    RAISE_APPLICATION_ERROR(-20002, 'Error: El cliente solo puede pedir a sucursales de su misma comunidad autonoma.');
                END IF;
            END;
            """;
        // 10. Para el cliente la fecha de suministro tendra que ser igual o posterior a la fecha de su ultimo suministro
        String triggerFechaSuministro = """
            CREATE OR REPLACE TRIGGER trg_Fecha_Suministro_Cliente
            BEFORE INSERT OR UPDATE ON Pide
            FOR EACH ROW
            DECLARE
                v_ultima_fecha_suministro DATE;
            BEGIN
                SELECT MAX(fecha_pide) INTO v_ultima_fecha_suministro 
                FROM Pide 
                WHERE cod_cliente = :NEW.cod_cliente
                
                IF v_ultima_fecha_suministro IS NOT NULL AND :NEW.fecha_pide < v_ultima_fecha_suministro THEN
                    RAISE_APPLICATION_ERROR(-20003, 'Error: La fecha de suministro debe ser igual o posterior a la ultima fecha de suministro.');
                END IF;
            END;
            """;
        
        // 15. Los datos de un vino solo se podran borrar si la cantidad total de suministrada de ese vino es cero
        String triggerBorrarVino = """
            CREATE OR REPLACE TRIGGER trg_Borrar_Vino_Suministrado
            BEFORE DELETE ON Vino
            FOR EACH ROW
            DECLARE
                v_total_suministrada NUMBER(8);
            BEGIN
                SELECT NVL(SUM(cantidad), 0) INTO v_total_suministrada 
                FROM Pide 
                WHERE cod_vino = :OLD.cod_vino;
                
                IF v_total_suministrada > 0 THEN
                    RAISE_APPLICATION_ERROR(-20004, 'Error: No se puede borrar el vino porque tiene cantidad suministrada mayor que cero.');
                END IF;
            END;
            """;

        // 16. Los datos de un productor solo se podran borrar si para cada vino que produce, la cantidad total suministrada es cero o no existe ningun suministro de ese vino
        String trgBorrarProductor = """
            CREATE OR REPLACE TRIGGER trg_Borrar_Productor_Ventas
            BEFORE DELETE ON Productor
            FOR EACH ROW
            DECLARE
                -- Cursor para recorrer los vinos de ese productor
                CURSOR cur_vinos IS SELECT cod_vino FROM Vino WHERE productor = :OLD.cod_p;
                v_total_vendido NUMBER;
            BEGIN
                FOR v IN cur_vinos LOOP
                    SELECT NVL(SUM(cantidad), 0) INTO v_total_vendido 
                    FROM Pide 
                    WHERE cod_vino = v.cod_vino;
                    
                    IF v_total_vendido > 0 THEN
                        RAISE_APPLICATION_ERROR(-20005, 'Error: No se puede borrar el productor: sus vinos se han vendido.');
                    END IF;
                END LOOP;
            END;
        """;

        String[] triggers = { 
            trgBorrarProductor, 
            triggerBorrarVino, 
            triggerFechaSuministro, 
            triggerClienteDelegacion, 
            triggerSalario 
        };

        for (DatabaseManager.Delegacion d : DatabaseManager.Delegacion.values()) {
            System.out.println("\nProcesando nodo: " + d);
            
            try (Connection conn = db.getConnection(d);
                 Statement stmt = conn.createStatement()) {

                System.out.print("Aplicando Checks... ");
                for (String sql : cheks) {
                    try {
                        stmt.executeUpdate(sql);
                    } catch (SQLException e) {
                        if (e.getErrorCode() != 2260 && e.getErrorCode() != 2275) {
                            System.err.println("Error: " + e.getMessage());
                        }
                    }
                }
                System.out.println("OK");

                System.out.print("Compilando Triggers... ");
                for (String sql : triggers) {
                    try {
                        stmt.execute(sql);
                    } catch (SQLException e) {
                        System.err.println("\nError compilando trigger: " + e.getMessage());
                    }
                }
                System.out.println("OK");

            } catch (SQLException e) {
                System.err.println("Error de conexión en " + d + ": " + e.getMessage());
            }
        }
        System.out.println("\nPROCESO TERMINADO.");
    }
}
