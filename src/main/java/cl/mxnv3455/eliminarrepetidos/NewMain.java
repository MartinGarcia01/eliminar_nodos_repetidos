/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.mxnv3455.eliminarrepetidos;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.*;

/**
 *
 * @author martin
 */
public class NewMain {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Neo4j db = new Neo4j("neo4j", "gabita");
        StatementResult res = db.ejecutarConsulta("MATCH (n)\n"
                + "return n;");
       
        while (res.hasNext()) {
            Record r = res.next();
            Node n = (Node) r.asMap().get("n");
            Iterator it_n = n.labels().iterator();
            String l_n = "";
            try {
                l_n = (String) it_n.next();
            } catch (NoSuchElementException e) {
                System.err.println(e.getMessage());
            }

            System.out.println(l_n);

            if (l_n.equals("Estado")) {
                
                db.ejecutarConsulta(generarConsultaReduccion(l_n, "PASO_POR", "idEstado", (String) n.asMap().get("idEstado")));

            } else if (l_n.equals("Reclamante")) {
              
                db.ejecutarConsulta(generarConsultaReduccion(l_n, "DENUNCIA_UN", "IDPersona", (String) n.asMap().get("IDPersona")));

            } else if (l_n.equals("Reclamado")) {
               
                db.ejecutarConsulta(generarConsultaReduccion(l_n, "PROCESA_A", "IDInstitucion", (String) n.asMap().get("IDInstitucion")));

            } else if (l_n.equals("Motivo")) {
             
                db.ejecutarConsulta(generarConsultaReduccion(l_n, "MOTIVADO_POR", "IdMotivo", (String) n.asMap().get("IdMotivo")));

            } else {
            }
            //generarConsultaReduccion(l_n, l_n, l_n, l_n);
        }
        db.close();
    }

    public static String generarConsultaReduccion(String claseNodo, String relacion, String id, String valorId) {
        String respuesta = "MATCH (n:" + claseNodo + " {" + id + ":\"" + valorId + "\" })\n"
                + "WITH collect(n) AS " + claseNodo + "s\n"
                + "WITH head(" + claseNodo + "s) AS super" + claseNodo + ", tail(" + claseNodo + "s) AS bad" + claseNodo + "s \n"
                + "UNWIND bad" + claseNodo + "s AS badNode\n"
                + "OPTIONAL MATCH (badNode)-[r]-(node)\n"
                + "DELETE r, badNode\n"
                + "WITH super" + claseNodo + ", collect(node) AS Denuncias\n"
                + "FOREACH (x IN Denuncias | MERGE (super" + claseNodo + ")-[:" + relacion + "]->(x))";
        return respuesta;
    }
}
