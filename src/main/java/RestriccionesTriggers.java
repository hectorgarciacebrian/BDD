import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class RestriccionesTriggers {
    public static void main(String[] args) {
        DatabaseManager db = DatabaseManager.getInstance();
        System.out.println("--- APLICANDO RESTRICCIONES Y TRIGGERS (CORREGIDO FINAL) ---");

        // 1. CHECKS ESTÁNDAR
        String[] checks = {
            "ALTER TABLE Cliente ADD CONSTRAINT CK_Tipo_Cliente CHECK (tipo_c IN ('A', 'B', 'C'))",
            "ALTER TABLE Vino ADD CONSTRAINT CK_Stock_Logico CHECK (stock >= 0 and stock <= c_producida)"
        };

        // 2. ELIMINACIÓN DE FKs LOCALES 
        // Eliminamos las FKs locales que apuntan a tablas fragmentadas (Vino, Sucursal)
        String[] dropFKs = {
            "ALTER TABLE Pide DROP CONSTRAINT FK_Pide_Vino",
            "ALTER TABLE Solicita DROP CONSTRAINT FK_Solicita_Vino",
            "ALTER TABLE Suministra DROP CONSTRAINT FK_Suministra_Vino",
            "ALTER TABLE Solicita DROP CONSTRAINT FK_Solicita_Suc_Prov"
        };

        // --- TRIGGERS DE INTEGRIDAD DISTRIBUIDA ---
        
        // INTEGRIDAD: Comprobar vino en Pide mirando la VISTA GLOBAL
        String trgIntegridadPide = """
            CREATE OR REPLACE TRIGGER trg_Integridad_Pide_Vino
            BEFORE INSERT OR UPDATE OF cod_vino ON Pide
            FOR EACH ROW
            DECLARE
                v_existe NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_existe FROM Vista_Vinos WHERE cod_vino = :NEW.cod_vino;
                IF v_existe = 0 THEN
                    RAISE_APPLICATION_ERROR(-20000, 'Error Integridad: El vino ' || :NEW.cod_vino || ' no existe en ninguna delegación.');
                END IF;
            END;
        """;

        // INTEGRIDAD: Comprobar vino en Solicita mirando la VISTA GLOBAL
        String trgIntegridadSolicita = """
            CREATE OR REPLACE TRIGGER trg_Integridad_Sol_Vino
            BEFORE INSERT OR UPDATE OF cod_tipo_vino ON Solicita
            FOR EACH ROW
            DECLARE
                v_existe NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_existe FROM Vista_Vinos WHERE cod_vino = :NEW.cod_tipo_vino;
                IF v_existe = 0 THEN
                    RAISE_APPLICATION_ERROR(-20000, 'Error Integridad: El vino ' || :NEW.cod_tipo_vino || ' no existe en ninguna delegación.');
                END IF;
            END;
        """;

        // INTEGRIDAD: Comprobar vino en Suministra mirando la VISTA GLOBAL
        String trgIntegridadSuministra = """
            CREATE OR REPLACE TRIGGER trg_Integridad_Sum_Vino
            BEFORE INSERT OR UPDATE OF cod_vino ON Suministra
            FOR EACH ROW
            DECLARE
                v_existe NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_existe FROM Vista_Vinos WHERE cod_vino = :NEW.cod_vino;
                IF v_existe = 0 THEN
                    RAISE_APPLICATION_ERROR(-20000, 'Error Integridad: El vino ' || :NEW.cod_vino || ' no existe en ninguna delegación.');
                END IF;
            END;
        """;

        // INTEGRIDAD: Comprobar sucursal en Solicita mirando la VISTA GLOBAL (CORREGIDO)
        String trgIntegridadSolicitaSuc = """
            CREATE OR REPLACE TRIGGER trg_Integridad_Sol_SucProv
            BEFORE INSERT OR UPDATE OF cod_sucursal_prov ON Solicita  -- Corregido: cod_sucursal_prov
            FOR EACH ROW
            DECLARE
                v_existe NUMBER;
            BEGIN
                -- Comprobamos en la VISTA GLOBAL, no en la tabla local
                SELECT COUNT(*) INTO v_existe FROM Vista_Sucursales WHERE cod_sucursal = :NEW.cod_sucursal_prov;
                
                IF v_existe = 0 THEN
                    RAISE_APPLICATION_ERROR(-20000, 'Error Integridad: La Sucursal proveedora ' || :NEW.cod_sucursal_prov || ' no existe en ninguna delegación.');
                END IF;
            END;
        """;

        // --- TRIGGERS DE REGLAS DE NEGOCIO ---

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
                v_ca_cliente  VARCHAR2(50);
                v_ca_sucursal VARCHAR2(50);
                v_delegacion_cliente NUMBER;
                v_delegacion_sucursal NUMBER;
                
                FUNCTION get_delegacion(p_ccaa VARCHAR2) RETURN NUMBER IS
                BEGIN
                    IF p_ccaa IN ('Castilla-León', 'Castilla-La Mancha', 'Aragón', 'Madrid', 'La Rioja') THEN RETURN 1;
                    ELSIF p_ccaa IN ('Cataluña', 'Baleares', 'País Valenciano', 'Murcia') THEN RETURN 2;
                    ELSIF p_ccaa IN ('Galicia', 'Asturias', 'Cantabria', 'País Vasco', 'Navarra') THEN RETURN 3;
                    ELSIF p_ccaa IN ('Andalucía', 'Extremadura', 'Canarias', 'Ceuta', 'Melilla') THEN RETURN 4;
                    ELSE RETURN 0;
                    END IF;
                END;
            BEGIN
                SELECT c_autonoma INTO v_ca_cliente FROM Vista_Clientes WHERE cod_c = :NEW.cod_cliente;
                SELECT c_autonoma INTO v_ca_sucursal FROM Vista_Sucursales WHERE cod_sucursal = :NEW.cod_sucursal;

                v_delegacion_cliente := get_delegacion(v_ca_cliente);
                v_delegacion_sucursal := get_delegacion(v_ca_sucursal);

                IF v_delegacion_cliente != v_delegacion_sucursal THEN
                    RAISE_APPLICATION_ERROR(-20002, 
                        'Error: Jurisdicción. Cliente de ' || v_ca_cliente || ' intentó comprar en ' || v_ca_sucursal);
                END IF;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                   RAISE_APPLICATION_ERROR(-20002, 'Error: Cliente o Sucursal no encontrados en el sistema global.');
            END;
        """;

        // 10. Fecha suministro >= ultima fecha
        String triggerFechaSuministro = """
            CREATE OR REPLACE TRIGGER trg_Fecha_Suministro_Cliente
            BEFORE INSERT OR UPDATE ON Pide
            FOR EACH ROW
            DECLARE
                v_ultima_fecha_suministro DATE;
            BEGIN
                SELECT MAX(fecha_pide) INTO v_ultima_fecha_suministro 
                FROM Pide 
                WHERE cod_cliente = :NEW.cod_cliente;
                
                IF v_ultima_fecha_suministro IS NOT NULL AND :NEW.fecha_pide < v_ultima_fecha_suministro THEN
                    RAISE_APPLICATION_ERROR(-20003, 'Error: La fecha de suministro debe ser igual o posterior a la ultima fecha de suministro.');
                END IF;
            END;
        """;
        
        // 15. Borrar Vino solo si suministrado es 0
        String triggerBorrarVino = """
            CREATE OR REPLACE TRIGGER trg_Borrar_Vino_Suministrado
            BEFORE DELETE ON Vino
            FOR EACH ROW
            DECLARE
                v_total_suministrada NUMBER(8);
            BEGIN
                SELECT NVL(SUM(cantidad), 0) INTO v_total_suministrada 
                FROM Vista_Suministros_Clientes 
                WHERE cod_vino = :OLD.cod_vino;
                
                IF v_total_suministrada > 0 THEN
                    RAISE_APPLICATION_ERROR(-20004, 'Error: No se puede borrar el vino porque tiene cantidad suministrada mayor que cero.');
                END IF;
            END;
        """;

        // 16. Borrar Productor
        String trgBorrarProductor = """
            CREATE OR REPLACE TRIGGER trg_Borrar_Productor_Ventas
            BEFORE DELETE ON Productor
            FOR EACH ROW
            DECLARE
                CURSOR cur_vinos IS SELECT cod_vino FROM Vino WHERE productor = :OLD.cod_p;
                v_total_vendido NUMBER;
            BEGIN
                FOR v IN cur_vinos LOOP
                    SELECT NVL(SUM(cantidad), 0) INTO v_total_vendido 
                    FROM Vista_Suministros_Clientes 
                    WHERE cod_vino = v.cod_vino;
                    
                    IF v_total_vendido > 0 THEN
                        RAISE_APPLICATION_ERROR(-20005, 'Error: No se puede borrar el productor: sus vinos se han vendido.');
                    END IF;
                END LOOP;
            END;
        """;

        // 17. Sucursal no pide a su misma delegación
        String trgMismaSucursal = """
            CREATE OR REPLACE TRIGGER trg_17_Delegacion
            BEFORE INSERT OR UPDATE ON Solicita
            FOR EACH ROW
            DECLARE
                v_ca1 VARCHAR2(50);
                v_ca2 VARCHAR2(50);
            BEGIN
                SELECT c_autonoma INTO v_ca1 FROM Vista_Sucursales WHERE cod_sucursal = :NEW.cod_sucursal;
                SELECT c_autonoma INTO v_ca2 FROM Vista_Sucursales WHERE cod_sucursal = :NEW.cod_sucursal_prov;
                
                IF v_ca1 = v_ca2 THEN
                    RAISE_APPLICATION_ERROR(-20017, 'Error: No se puede pedir a una sucursal de la misma delegacion.');
                END IF;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                   RAISE_APPLICATION_ERROR(-20017, 'Error: Alguna de las sucursales no existe en el sistema global.');
            END;
        """;

        // 18. Cantidad pedida <= demanda (OPCIONAL: Coméntalo si falla la carga inicial)
        String trgCantidadDemanda = """
           CREATE OR REPLACE TRIGGER trg_validacion_pedido_R18
            BEFORE INSERT OR UPDATE ON Solicita
            FOR EACH ROW
            DECLARE
                v_total_demandado_clientes NUMBER := 0;
                v_total_ya_pedido_sucursales NUMBER := 0;
            BEGIN
                SELECT NVL(SUM(cantidad), 0)
                INTO v_total_demandado_clientes
                FROM Vista_Suministros_Clientes
                WHERE cod_sucursal = :NEW.cod_sucursal
                AND cod_vino = :NEW.cod_tipo_vino; 

                SELECT NVL(SUM(cantidad), 0)
                INTO v_total_ya_pedido_sucursales
                FROM Vista_Pedidos_Entre_Sucursales
                WHERE Suc_Sol = :NEW.cod_sucursal   
                AND Vino = :NEW.cod_tipo_vino;    

                IF (v_total_ya_pedido_sucursales + :NEW.cantidad) > v_total_demandado_clientes THEN
                    RAISE_APPLICATION_ERROR(-20018, 
                        'Error R18: Stock excedido. Demanda: ' || v_total_demandado_clientes || 
                        '. Acumulado + Nuevo: ' || (v_total_ya_pedido_sucursales + :NEW.cantidad));
                END IF;
            END;
        """;

        // 19. Pedir a zona de origen
        String trgPedirMadrid = """
            CREATE OR REPLACE TRIGGER trg_19_Pedir_Al_Origen
            BEFORE INSERT OR UPDATE ON Solicita
            FOR EACH ROW
            DECLARE
                v_ca_vino       VARCHAR2(50);
                v_ca_proveedora VARCHAR2(50);
                
                FUNCTION get_delegacion_zona(p_ccaa VARCHAR2) RETURN VARCHAR2 IS
                BEGIN
                    IF p_ccaa IN ('Castilla-León', 'Castilla-La Mancha', 'Aragón', 'Madrid', 'La Rioja') THEN RETURN 'CENTRO';
                    ELSIF p_ccaa IN ('Cataluña', 'Baleares', 'País Valenciano', 'Murcia') THEN RETURN 'LEVANTE';
                    ELSIF p_ccaa IN ('Galicia', 'Asturias', 'Cantabria', 'País Vasco', 'Navarra') THEN RETURN 'NORTE';
                    ELSIF p_ccaa IN ('Andalucía', 'Extremadura', 'Canarias', 'Ceuta', 'Melilla') THEN RETURN 'SUR';
                    ELSE RETURN 'OTRO';
                    END IF;
                END;
            BEGIN
                SELECT c_autonoma INTO v_ca_vino       FROM Vista_Vinos      WHERE cod_vino     = :NEW.cod_tipo_vino;
                SELECT c_autonoma INTO v_ca_proveedora FROM Vista_Sucursales WHERE cod_sucursal = :NEW.cod_sucursal_prov;

                IF get_delegacion_zona(v_ca_vino) != get_delegacion_zona(v_ca_proveedora) THEN
                    RAISE_APPLICATION_ERROR(-20019, 
                        'Error: Debes pedir el vino directamente a su zona de origen (' || get_delegacion_zona(v_ca_vino) || ')');
                END IF;

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                   RAISE_APPLICATION_ERROR(-20020, 'Error: Datos no encontrados al validar origen del vino.');
            END;
        """;

        // 20. Fecha pedido posterior al ultimo pedido
        String trgFechaOrden = """
            CREATE OR REPLACE TRIGGER trg_validar_fechas_R20
            BEFORE INSERT OR UPDATE ON Solicita
            FOR EACH ROW
            DECLARE
                v_ultima_fecha DATE;
            BEGIN
                SELECT MAX(fecha_sol)
                INTO v_ultima_fecha
                FROM Vista_Pedidos_Entre_Sucursales
                WHERE Suc_Sol = :NEW.cod_sucursal       
                AND Suc_Prov = :NEW.cod_sucursal_prov     
                AND Vino = :NEW.cod_tipo_vino;            

                IF v_ultima_fecha IS NOT NULL AND :NEW.fecha_sol <= v_ultima_fecha THEN
                    RAISE_APPLICATION_ERROR(-20020,
                        'Error R20: Fecha inválida. El nuevo pedido debe ser posterior al ' || TO_CHAR(v_ultima_fecha, 'DD/MM/YYYY'));
                END IF;
            END;
        """;

        // 21. Fecha solicitud posterior a ultima petición cliente
        String trgFechaCliente = """
            CREATE OR REPLACE TRIGGER trg_21_Fecha_Cliente
            BEFORE INSERT OR UPDATE ON Solicita
            FOR EACH ROW
            DECLARE
                v_max_fecha_cli DATE;
            BEGIN
                SELECT MAX(fecha_pide) INTO v_max_fecha_cli
                FROM Pide
                WHERE cod_sucursal = :NEW.cod_sucursal AND cod_vino = :NEW.cod_tipo_vino;

                IF v_max_fecha_cli IS NOT NULL AND :NEW.fecha_sol <= v_max_fecha_cli THEN
                    RAISE_APPLICATION_ERROR(-20021, 'Error: La solicitud debe ser posterior a la ultima demanda del cliente.');
                END IF;
            END;
        """;

        String[] triggers = { 
            // Integridad Distribuida
            trgIntegridadPide,
            trgIntegridadSolicita,
            trgIntegridadSuministra,
            trgIntegridadSolicitaSuc,
            // Negocio
            trgBorrarProductor, 
            triggerBorrarVino, 
            triggerFechaSuministro, 
            triggerClienteDelegacion, 
            triggerSalario,
            trgMismaSucursal,
            trgCantidadDemanda, 
            trgPedirMadrid,
            trgFechaOrden,
            trgFechaCliente 
        };

        for (DatabaseManager.Delegacion d : DatabaseManager.Delegacion.values()) {
            System.out.println("\nProcesando nodo: " + d);
            
            try (Connection conn = db.getConnection(d);
                 Statement stmt = conn.createStatement()) {

                System.out.print("Aplicando Checks... ");
                for (String sql : checks) {
                    try {
                        stmt.executeUpdate(sql);
                    } catch (SQLException e) {
                         if (e.getErrorCode() != 2260 && e.getErrorCode() != 2275) {} // Ignorar si ya existen
                    }
                }
                System.out.println("OK");

                System.out.print("Eliminando FKs Locales (Vino/Sucursal)... ");
                for (String sql : dropFKs) {
                    try {
                        stmt.executeUpdate(sql);
                    } catch (SQLException e) {
                         if (e.getErrorCode() != 2443) {} // Ignorar si no existen
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