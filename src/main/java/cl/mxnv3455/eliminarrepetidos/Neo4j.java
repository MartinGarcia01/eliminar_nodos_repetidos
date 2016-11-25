/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.mxnv3455.eliminarrepetidos;
import java.util.List;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
/**
 *
 * @author martin
 */
public class Neo4j {
    private Driver driver;
    private Session session;
    private int numeroInsercion;

    public Neo4j(String user, String pass) {
        this.driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic(user, pass));
        this.session = driver.session();
        
    }
    public StatementResult ejecutarConsulta(String consulta){
        StatementResult exito=null;
        try {
            exito = session.run(consulta);
            
            
        } catch (Exception e) {
            
            System.err.println(e.getMessage());
        }
        return exito;
    }
    
    public void close(){
        this.session.close();
        this.driver.close();
    }
    
}
