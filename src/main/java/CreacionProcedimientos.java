import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreacionProcedimientos {

    public static void main(String[] args) {
        DatabaseManager db = DatabaseManager.getInstance();

        String[] procedures = {
            
            // Procedimiento auxiliar para obtener delegación según CCAA
            """
            CREATE OR REPLACE PROCEDURE PR_GET_DELEGACION (
                xDelegacion OUT VARCHAR2, 
                xCCAA       IN  Sucursal.c_autonoma%TYPE
            ) IS
            BEGIN
                IF (xCCAA = 'Castilla-León' OR xCCAA = 'Castilla-La Mancha' OR xCCAA = 'Aragón' 
                    OR xCCAA = 'Madrid' OR xCCAA = 'La Rioja') THEN
                        xDelegacion := 'Madrid';
                ELSIF (xCCAA = 'Cataluña' OR xCCAA = 'Baleares' OR xCCAA = 'País Valenciano' 
                    OR xCCAA = 'Murcia') THEN
                        xDelegacion := 'Barcelona';
                ELSIF (xCCAA = 'Galicia' OR xCCAA = 'Asturias' OR xCCAA = 'Cantabria' 
                    OR xCCAA = 'País Vasco' OR xCCAA = 'Navarra') THEN      
                        xDelegacion := 'La Coruña';  
                ELSIF (xCCAA = 'Andalucía' OR xCCAA = 'Extremadura' OR xCCAA = 'Canarias' 
                    OR xCCAA = 'Ceuta' OR xCCAA = 'Melilla') THEN
                        xDelegacion := 'Sevilla';
                ELSE 
                    RAISE_APPLICATION_ERROR(-20204, 'Error: La Comunidad Autonoma (' || xCCAA || ') no es correcta o no esta asignada.');
                END IF;
            END;
            """,

            // 1. Alta Empleado
            """
            CREATE OR REPLACE PROCEDURE pr_Alta_Empleado (
                p_cod_e      IN NUMBER,
                p_dni        IN VARCHAR2,
                p_nombre     IN VARCHAR2,
                p_fecha      IN DATE,
                p_salario    IN NUMBER,
                p_direccion  IN VARCHAR2,
                p_sucursal   IN NUMBER
            ) IS
                v_existe NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_existe FROM vista_empleados WHERE cod_e = p_cod_e OR dni_e = p_dni;
                IF v_existe > 0 THEN
                    RAISE_APPLICATION_ERROR(-20006, 'Error: Ya existe un empleado con ese Codigo o DNI.');
                END IF;
                -- Si no existe, insertamos
                INSERT INTO Empleado (cod_e, dni_e, nombre_e, fecha_comp, salario, direccion_e, sucursal_dest)
                VALUES (p_cod_e, p_dni, p_nombre, p_fecha, p_salario, p_direccion, p_sucursal);
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    RAISE_APPLICATION_ERROR(-20006, 'Error: Ya existe un empleado con ese Codigo o DNI.');
            END;
            """,

            // 2. Baja Empleado (DISTRIBUIDO)
            """
            CREATE OR REPLACE PROCEDURE pr_Baja_Empleado (
                p_cod_e IN NUMBER
            ) IS
                v_deleted BOOLEAN := FALSE;
            BEGIN
                -- 1. Desvincular como director en TODAS las sucursales (podría dirigir una sucursal en otro nodo)
                UPDATE cerveza1.Sucursal SET director = NULL WHERE director = p_cod_e;
                UPDATE cerveza2.Sucursal SET director = NULL WHERE director = p_cod_e;
                UPDATE cerveza3.Sucursal SET director = NULL WHERE director = p_cod_e;
                UPDATE cerveza4.Sucursal SET director = NULL WHERE director = p_cod_e;

                -- 2. Borrar Empleado (Buscamos secuencialmente dónde está)
                DELETE FROM cerveza1.Empleado WHERE cod_e = p_cod_e;
                IF SQL%ROWCOUNT > 0 THEN v_deleted := TRUE; END IF;

                IF NOT v_deleted THEN
                    DELETE FROM cerveza2.Empleado WHERE cod_e = p_cod_e;
                    IF SQL%ROWCOUNT > 0 THEN v_deleted := TRUE; END IF;
                END IF;

                IF NOT v_deleted THEN
                    DELETE FROM cerveza3.Empleado WHERE cod_e = p_cod_e;
                    IF SQL%ROWCOUNT > 0 THEN v_deleted := TRUE; END IF;
                END IF;

                IF NOT v_deleted THEN
                    DELETE FROM cerveza4.Empleado WHERE cod_e = p_cod_e;
                    IF SQL%ROWCOUNT > 0 THEN v_deleted := TRUE; END IF;
                END IF;

                IF NOT v_deleted THEN
                    RAISE_APPLICATION_ERROR(-20007, 'Error: No existe el empleado a borrar en ninguna delegación.');
                END IF;
            END;
            """,

            // 3. Modificar Salario
            """
            CREATE OR REPLACE PROCEDURE pr_Modificar_Salario (
                p_cod_e     IN NUMBER,
                p_nuevo_sal IN NUMBER
            ) IS
            BEGIN
                UPDATE Empleado SET salario = p_nuevo_sal WHERE cod_e = p_cod_e;
                IF SQL%ROWCOUNT = 0 THEN
                    RAISE_APPLICATION_ERROR(-20008, 'Error: Empleado no encontrado.');
                END IF;
            END;
            """,

            // 4. Trasladar Empleado
            """
            CREATE OR REPLACE PROCEDURE pr_Trasladar_Empleado (
                p_cod_e     IN NUMBER,
                p_nueva_suc IN NUMBER,
                p_nueva_dir IN VARCHAR2 DEFAULT NULL
            ) IS
            BEGIN
                UPDATE Empleado 
                SET sucursal_dest = p_nueva_suc,
                    direccion_e   = p_nueva_dir
                WHERE cod_e = p_cod_e;
                IF SQL%ROWCOUNT = 0 THEN
                    RAISE_APPLICATION_ERROR(-20009, 'Error: Empleado no encontrado para traslado.');
                END IF;
            END;
            """,

            // 5. Alta Sucursal
            """
            CREATE OR REPLACE PROCEDURE pr_Alta_Sucursal (
                p_cod_suc    IN NUMBER,
                p_nombre     IN VARCHAR2,
                p_ciudad     IN VARCHAR2,
                p_c_autonoma IN VARCHAR2,
                p_director   IN NUMBER DEFAULT NULL
            ) IS
                v_delegacion_calc VARCHAR2(50);
                v_existe NUMBER;
            BEGIN
                SELECT COUNT (*) INTO v_existe FROM vista_sucursales WHERE cod_sucursal = p_cod_suc;
                IF v_existe > 0 THEN
                    RAISE_APPLICATION_ERROR(-20010, 'Error: Ya existe una sucursal con ese Codigo.');
                END IF;
                PR_GET_DELEGACION(v_delegacion_calc, p_c_autonoma);
                INSERT INTO Sucursal (cod_sucursal, nombre, ciudad, c_autonoma, director)
                VALUES (p_cod_suc, p_nombre, p_ciudad, p_c_autonoma, p_director);
            END;
            """,

            // 6. Cambiar Director
            """
            CREATE OR REPLACE PROCEDURE pr_Cambiar_Director (
                p_cod_suc      IN NUMBER,
                p_nuevo_dir_e  IN NUMBER
            ) IS
            BEGIN
                UPDATE Sucursal SET director = p_nuevo_dir_e WHERE cod_sucursal = p_cod_suc;
                IF SQL%ROWCOUNT = 0 THEN
                    RAISE_APPLICATION_ERROR(-20010, 'Error: Sucursal no encontrada.');
                END IF;
            END;
            """,

            // 7. Alta Cliente
            """
            CREATE OR REPLACE PROCEDURE pr_Alta_Cliente (
                p_cod_c      IN NUMBER,
                p_dni        IN VARCHAR2,
                p_nombre     IN VARCHAR2,
                p_direccion  IN VARCHAR2,
                p_tipo       IN VARCHAR2,
                p_c_autonoma IN VARCHAR2
            ) IS
                v_check_delegacion VARCHAR2(50);
                v_existe NUMBER;
            BEGIN
                SELECT COUNT (*) INTO v_existe FROM vista_clientes WHERE cod_c = p_cod_c;
                IF v_existe > 0 THEN
                    RAISE_APPLICATION_ERROR(-20011, 'Error: Cliente ya existe.');
                END IF;
                PR_GET_DELEGACION(v_check_delegacion, p_c_autonoma);
                INSERT INTO Cliente (cod_c, dni_c, nombre_c, direccion_c, tipo_c, c_autonoma)
                VALUES (p_cod_c, p_dni, p_nombre, p_direccion, p_tipo, p_c_autonoma);
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    RAISE_APPLICATION_ERROR(-20011, 'Error: Cliente ya existe.');
            END;
            """,

            // 8. Gestionar Suministro (CORREGIDO PARA EVITAR ORA-01732)
            """
            CREATE OR REPLACE PROCEDURE pr_Gestionar_Suministro (
                p_cod_c    IN NUMBER,
                p_cod_suc  IN NUMBER,
                p_cod_vino IN NUMBER,
                p_fecha    IN DATE,
                p_cantidad IN NUMBER
            ) IS
                v_existe NUMBER;
                v_updated BOOLEAN := FALSE;
            BEGIN
                -- 1. Insertar o Actualizar Pide (Local)
                SELECT COUNT(*) INTO v_existe
                FROM Pide
                WHERE cod_cliente = p_cod_c AND cod_sucursal = p_cod_suc 
                  AND cod_vino = p_cod_vino AND fecha_pide = p_fecha;

                IF v_existe > 0 THEN
                    UPDATE Pide 
                    SET cantidad = cantidad + p_cantidad
                    WHERE cod_cliente = p_cod_c AND cod_sucursal = p_cod_suc 
                      AND cod_vino = p_cod_vino AND fecha_pide = p_fecha;
                ELSE
                    INSERT INTO Pide (cod_cliente, cod_sucursal, cod_vino, fecha_pide, cantidad)
                    VALUES (p_cod_c, p_cod_suc, p_cod_vino, p_fecha, p_cantidad);
                END IF;

                -- 2. Actualizar Stock en el nodo correcto (Probamos secuencialmente)
                UPDATE cerveza1.Vino SET stock = stock - p_cantidad WHERE cod_vino = p_cod_vino;
                IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza2.Vino SET stock = stock - p_cantidad WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza3.Vino SET stock = stock - p_cantidad WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza4.Vino SET stock = stock - p_cantidad WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;
                
                IF NOT v_updated THEN
                     RAISE_APPLICATION_ERROR(-20012, 'Error: El vino ' || p_cod_vino || ' no existe en ninguna delegacion.');
                END IF;
            END;
            """,

            // 9. Baja Suministros (CORREGIDO PARA EVITAR ORA-01732)
            """
            CREATE OR REPLACE PROCEDURE pr_Baja_Suministros (
                p_cod_c    IN NUMBER,
                p_cod_suc  IN NUMBER,
                p_cod_vino IN NUMBER,
                p_fecha    IN DATE DEFAULT NULL
            ) IS
                v_cantidad_restaurar NUMBER := 0;
                v_updated BOOLEAN := FALSE;
            BEGIN
                -- 1. Calcular cantidad y borrar de Pide
                IF p_fecha IS NOT NULL THEN
                    SELECT NVL(SUM(cantidad),0) INTO v_cantidad_restaurar
                    FROM Pide
                    WHERE cod_cliente = p_cod_c AND cod_sucursal = p_cod_suc 
                      AND cod_vino = p_cod_vino AND fecha_pide = p_fecha;
                      
                    IF v_cantidad_restaurar = 0 THEN
                        RAISE_APPLICATION_ERROR(-20013, 'Error: No hay suministro en esa fecha para borrar.');
                    END IF;
                    
                    DELETE FROM Pide 
                    WHERE cod_cliente = p_cod_c AND cod_sucursal = p_cod_suc 
                      AND cod_vino = p_cod_vino AND fecha_pide = p_fecha;
                ELSE
                    SELECT NVL(SUM(cantidad),0) INTO v_cantidad_restaurar
                    FROM Pide
                    WHERE cod_cliente = p_cod_c AND cod_sucursal = p_cod_suc 
                      AND cod_vino = p_cod_vino;
                      
                    IF v_cantidad_restaurar = 0 THEN
                        RAISE_APPLICATION_ERROR(-20013, 'Error: No existen suministros de ese vino para borrar.');
                    END IF;

                    DELETE FROM Pide 
                    WHERE cod_cliente = p_cod_c AND cod_sucursal = p_cod_suc 
                      AND cod_vino = p_cod_vino;
                END IF;

                -- 2. Restaurar Stock (Secuencial en los 4 nodos)
                UPDATE cerveza1.Vino SET stock = stock + v_cantidad_restaurar WHERE cod_vino = p_cod_vino;
                IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza2.Vino SET stock = stock + v_cantidad_restaurar WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza3.Vino SET stock = stock + v_cantidad_restaurar WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza4.Vino SET stock = stock + v_cantidad_restaurar WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;
            END;
            """,

            // 10. Alta Pedido Sucursal (CORREGIDO PARA EVITAR ORA-01732)
            """
            CREATE OR REPLACE PROCEDURE pr_Alta_Pedido_Suc (
                p_cod_suc_pide IN NUMBER,
                p_cod_suc_prov IN NUMBER,
                p_cod_vino     IN NUMBER,
                p_fecha        IN DATE,
                p_cantidad     IN NUMBER
            ) IS
                v_updated BOOLEAN := FALSE;
            BEGIN
                INSERT INTO Solicita (cod_sucursal, cod_sucursal_prov, cod_tipo_vino, fecha_sol, cantidad)
                VALUES (p_cod_suc_pide, p_cod_suc_prov, p_cod_vino, p_fecha, p_cantidad);

                -- Actualizar Stock secuencialmente
                UPDATE cerveza1.Vino SET stock = stock - p_cantidad WHERE cod_vino = p_cod_vino;
                IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza2.Vino SET stock = stock - p_cantidad WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza3.Vino SET stock = stock - p_cantidad WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza4.Vino SET stock = stock - p_cantidad WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;
                
                IF NOT v_updated THEN
                    RAISE_APPLICATION_ERROR(-20014, 'Error: Vino no encontrado en ninguna delegacion para actualizar stock.');
                END IF;
            END;
            """,
            
            // 11. Baja Pedido Sucursal (CORREGIDO PARA EVITAR ORA-01732)
            """
            CREATE OR REPLACE PROCEDURE pr_Baja_Pedido_Suc (
                p_cod_suc_pide IN NUMBER,
                p_cod_suc_prov IN NUMBER,
                p_cod_vino     IN NUMBER,
                p_fecha        IN DATE DEFAULT NULL
            ) IS
                v_cantidad_restaurar NUMBER := 0;
                v_updated BOOLEAN := FALSE;
            BEGIN
                IF p_fecha IS NOT NULL THEN
                    SELECT NVL(SUM(cantidad),0) INTO v_cantidad_restaurar
                    FROM Solicita
                    WHERE cod_sucursal = p_cod_suc_pide AND cod_sucursal_prov = p_cod_suc_prov 
                      AND cod_tipo_vino = p_cod_vino AND fecha_sol = p_fecha;
                      
                    IF v_cantidad_restaurar = 0 THEN
                        RAISE_APPLICATION_ERROR(-20015, 'Error: No hay pedido en esa fecha para borrar.');
                    END IF;
                    
                    DELETE FROM Solicita 
                    WHERE cod_sucursal = p_cod_suc_pide AND cod_sucursal_prov = p_cod_suc_prov 
                      AND cod_tipo_vino = p_cod_vino AND fecha_sol = p_fecha;
                ELSE
                    SELECT NVL(SUM(cantidad),0) INTO v_cantidad_restaurar
                    FROM Solicita
                    WHERE cod_sucursal = p_cod_suc_pide AND cod_sucursal_prov = p_cod_suc_prov 
                      AND cod_tipo_vino = p_cod_vino;
                      
                    IF v_cantidad_restaurar = 0 THEN
                        RAISE_APPLICATION_ERROR(-20015, 'Error: No existen pedidos de ese vino para borrar.');
                    END IF;

                    DELETE FROM Solicita 
                    WHERE cod_sucursal = p_cod_suc_pide AND cod_sucursal_prov = p_cod_suc_prov 
                      AND cod_tipo_vino = p_cod_vino;
                END IF;

                -- Restaurar Stock secuencialmente
                UPDATE cerveza1.Vino SET stock = stock + v_cantidad_restaurar WHERE cod_vino = p_cod_vino;
                IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza2.Vino SET stock = stock + v_cantidad_restaurar WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza3.Vino SET stock = stock + v_cantidad_restaurar WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;

                IF NOT v_updated THEN
                    UPDATE cerveza4.Vino SET stock = stock + v_cantidad_restaurar WHERE cod_vino = p_cod_vino;
                    IF SQL%ROWCOUNT > 0 THEN v_updated := TRUE; END IF;
                END IF;
            END;
            """,

            // 12. Alta Vino
            """
            CREATE OR REPLACE PROCEDURE pr_Alta_Vino (
                p_cod_vino     IN NUMBER,
                p_marca        IN VARCHAR2,
                p_anio         IN NUMBER,
                p_denominacion IN VARCHAR2,
                p_graduacion   IN NUMBER,
                p_vinedo       IN VARCHAR2,
                p_c_autonoma   IN VARCHAR2,
                p_cantidad     IN NUMBER,   
                p_productor    IN NUMBER
            ) IS
                v_dummy VARCHAR2(50);
            BEGIN
                PR_GET_DELEGACION(v_dummy, p_c_autonoma);

                INSERT INTO Vino (
                    cod_vino, nombre_v, anio, denominacion, graduacion, 
                    vinedo, c_autonoma, c_producida, stock, productor
                )
                VALUES (
                    p_cod_vino, p_marca, p_anio, p_denominacion, p_graduacion, 
                    p_vinedo, p_c_autonoma, p_cantidad, p_cantidad, p_productor
                );
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    RAISE_APPLICATION_ERROR(-20016, 'Error: Ya existe un vino con ese código.');
            END;
            """,
            
            // 13. Baja Vino (DISTRIBUIDO)
            """
            CREATE OR REPLACE PROCEDURE pr_Baja_Vino (
                p_cod_vino IN NUMBER
            ) IS
                v_stock NUMBER;
                v_count NUMBER := 0;
                v_encontrado BOOLEAN := FALSE;
                v_nodo_origen VARCHAR2(10);
            BEGIN
                -- 1. VERIFICAR STOCK (Buscamos dónde está el vino y si tiene stock)
                
                -- Nodo 1
                BEGIN
                    SELECT stock INTO v_stock FROM cerveza1.Vino WHERE cod_vino = p_cod_vino;
                    v_encontrado := TRUE; v_nodo_origen := 'cerveza1';
                    IF v_stock > 0 THEN RAISE_APPLICATION_ERROR(-20020, 'No se puede borrar: El vino tiene STOCK ('||v_stock||') en cerveza1.'); END IF;
                EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;

                -- Nodo 2
                IF NOT v_encontrado THEN
                    BEGIN
                        SELECT stock INTO v_stock FROM cerveza2.Vino WHERE cod_vino = p_cod_vino;
                        v_encontrado := TRUE; v_nodo_origen := 'cerveza2';
                        IF v_stock > 0 THEN RAISE_APPLICATION_ERROR(-20020, 'No se puede borrar: El vino tiene STOCK ('||v_stock||') en cerveza2.'); END IF;
                    EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
                END IF;

                -- Nodo 3
                IF NOT v_encontrado THEN
                    BEGIN
                        SELECT stock INTO v_stock FROM cerveza3.Vino WHERE cod_vino = p_cod_vino;
                        v_encontrado := TRUE; v_nodo_origen := 'cerveza3';
                        IF v_stock > 0 THEN RAISE_APPLICATION_ERROR(-20020, 'No se puede borrar: El vino tiene STOCK ('||v_stock||') en cerveza3.'); END IF;
                    EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
                END IF;

                -- Nodo 4
                IF NOT v_encontrado THEN
                    BEGIN
                        SELECT stock INTO v_stock FROM cerveza4.Vino WHERE cod_vino = p_cod_vino;
                        v_encontrado := TRUE; v_nodo_origen := 'cerveza4';
                        IF v_stock > 0 THEN RAISE_APPLICATION_ERROR(-20020, 'No se puede borrar: El vino tiene STOCK ('||v_stock||') en cerveza4.'); END IF;
                    EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;
                END IF;

                IF NOT v_encontrado THEN
                    RAISE_APPLICATION_ERROR(-20017, 'Error: El vino no existe en ninguna delegación.');
                END IF;

                -- 2. VERIFICAR SI HAY PEDIDOS O SUMINISTROS (Revisar los 4 nodos)
                -- Sumamos todas las apariciones en las tablas Pide, Solicita y Suministra de todos los nodos
                
                -- Check Cerveza 1
                SELECT count(*) INTO v_count FROM cerveza1.Pide WHERE cod_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: Hay pedidos de clientes (Pide) en cerveza1.'); END IF;
                
                SELECT count(*) INTO v_count FROM cerveza1.Solicita WHERE cod_tipo_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: Hay pedidos entre sucursales (Solicita) en cerveza1.'); END IF;

                SELECT count(*) INTO v_count FROM cerveza1.Suministra WHERE cod_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: El vino está en catálogos de suministro en cerveza1.'); END IF;

                -- Check Cerveza 2
                SELECT count(*) INTO v_count FROM cerveza2.Pide WHERE cod_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: Hay pedidos de clientes (Pide) en cerveza2.'); END IF;
                
                SELECT count(*) INTO v_count FROM cerveza2.Solicita WHERE cod_tipo_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: Hay pedidos entre sucursales (Solicita) en cerveza2.'); END IF;

                SELECT count(*) INTO v_count FROM cerveza2.Suministra WHERE cod_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: El vino está en catálogos de suministro en cerveza2.'); END IF;

                -- Check Cerveza 3
                SELECT count(*) INTO v_count FROM cerveza3.Pide WHERE cod_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: Hay pedidos de clientes (Pide) en cerveza3.'); END IF;
                
                SELECT count(*) INTO v_count FROM cerveza3.Solicita WHERE cod_tipo_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: Hay pedidos entre sucursales (Solicita) en cerveza3.'); END IF;

                SELECT count(*) INTO v_count FROM cerveza3.Suministra WHERE cod_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: El vino está en catálogos de suministro en cerveza3.'); END IF;

                -- Check Cerveza 4
                SELECT count(*) INTO v_count FROM cerveza4.Pide WHERE cod_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: Hay pedidos de clientes (Pide) en cerveza4.'); END IF;
                
                SELECT count(*) INTO v_count FROM cerveza4.Solicita WHERE cod_tipo_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: Hay pedidos entre sucursales (Solicita) en cerveza4.'); END IF;

                SELECT count(*) INTO v_count FROM cerveza4.Suministra WHERE cod_vino = p_cod_vino;
                IF v_count > 0 THEN RAISE_APPLICATION_ERROR(-20021, 'No se puede borrar: El vino está en catálogos de suministro en cerveza4.'); END IF;

                -- 3. BORRAR EL VINO (Si hemos llegado aquí, es seguro borrar)
                IF v_nodo_origen = 'cerveza1' THEN DELETE FROM cerveza1.Vino WHERE cod_vino = p_cod_vino; END IF;
                IF v_nodo_origen = 'cerveza2' THEN DELETE FROM cerveza2.Vino WHERE cod_vino = p_cod_vino; END IF;
                IF v_nodo_origen = 'cerveza3' THEN DELETE FROM cerveza3.Vino WHERE cod_vino = p_cod_vino; END IF;
                IF v_nodo_origen = 'cerveza4' THEN DELETE FROM cerveza4.Vino WHERE cod_vino = p_cod_vino; END IF;

            END;
            """,
            
            // 14. Alta Productor
            """
            CREATE OR REPLACE PROCEDURE pr_Alta_Productor (
                p_cod_p       IN NUMBER,
                p_dni_p       IN VARCHAR2,
                p_nombre_p    IN VARCHAR2,
                p_direccion_p IN VARCHAR2
            ) IS
                v_existe NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_existe FROM Vista_Productores WHERE cod_p = p_cod_p AND dni_p != p_dni_p;
                IF v_existe > 0 THEN
                    RAISE_APPLICATION_ERROR(-20018, 'Error: Ya existe productor con ese Código o DNI.');
                END IF;
                INSERT INTO Productor (cod_p, dni_p, nombre_p, direccion_p)
                VALUES (p_cod_p, p_dni_p, p_nombre_p, p_direccion_p);
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    RAISE_APPLICATION_ERROR(-20018, 'Error: Ya existe productor con ese Código o DNI.');
            END;
            """,

            // 15. Baja Productor
            """
            CREATE OR REPLACE PROCEDURE pr_Baja_Productor (
                p_cod_p IN NUMBER
            ) IS
            BEGIN
                DELETE FROM Productor WHERE cod_p = p_cod_p;
                IF SQL%ROWCOUNT = 0 THEN
                    RAISE_APPLICATION_ERROR(-20019, 'Error: No existe el productor a borrar.');
                END IF;
            END;
            """
        };

        System.out.println("--- CREACIÓN DE PROCEDIMIENTOS ALMACENADOS (VERSIÓN DISTRIBUIDA MANUAL) ---");

        for (DatabaseManager.Delegacion d : DatabaseManager.Delegacion.values()) {
            System.out.println("\nProcesando nodo: " + d);
            try (Connection conn = db.getConnection(d);
                 Statement stmt = conn.createStatement()) {

                for (int i = 0; i < procedures.length; i++) {
                    try {
                        stmt.execute(procedures[i]);
                    } catch (SQLException e) {
                        System.err.println("Error en procedimiento " + (i) + ": " + e.getMessage());
                    }
                }
                System.out.println("Procedimientos compilados OK.");

            } catch (SQLException e) {
                System.err.println("Error de conexión: " + e.getMessage());
            }
        }
    }
}