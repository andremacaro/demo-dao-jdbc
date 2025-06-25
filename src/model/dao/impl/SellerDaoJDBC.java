package model.dao.impl;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SellerDaoJDBC implements SellerDao {

    private Connection conn;

    public SellerDaoJDBC(Connection conn){
        this.conn = conn;
    }

    @Override
    public void insert(Seller obj) {
        PreparedStatement st = null;
        try{
            st = conn.prepareStatement("INSERT INTO seller " +
                                            "(Name, Email, BirthDate, BaseSalary, DepartmentId) " +
                                            "VALUES " +
                                            "(?, ?, ?, ?, ?) ",
                                            Statement.RETURN_GENERATED_KEYS);

            st.setString(1, obj.getName());
            st.setString(2, obj.getEmail());
            st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
            st.setDouble(4, obj.getBaseSalary());
            st.setInt(5, obj.getDepartment().getId());

            int rowsAffected = st.executeUpdate();

            if (rowsAffected > 0){
                ResultSet rs = st.getGeneratedKeys();
                if (rs.next()){
                    int id = rs.getInt(1);
                    obj.setId(id);
                }
                DB.closeResultSet(rs);
            }
            else {
                throw new DbException("Unexpected error! No rows affected!");
            }
        }
        catch (SQLException e){
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(st);
        }
    }

    @Override
    public void update(Seller obj) {
        PreparedStatement st = null;
        try{
            st = conn.prepareStatement("UPDATE seller " +
                            "SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId  = ? " +
                            "WHERE Id = ? ");

            st.setString(1, obj.getName());
            st.setString(2, obj.getEmail());
            st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
            st.setDouble(4, obj.getBaseSalary());
            st.setInt(5, obj.getDepartment().getId());
            st.setInt(6, obj.getId());

            st.executeUpdate();
        }
        catch (SQLException e){
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(st);
        }
    }

    @Override
    public void deleteById(Integer id) {

    }

    @Override
    public Seller findById(Integer id) {
        PreparedStatement st = null;
        ResultSet rs = null;

        try{
            st = conn.prepareStatement(
                        "SELECT seller.*,department.Name as DepName " +
                            "FROM seller INNER JOIN department " +
                            "ON seller.DepartmentId = department.Id " +
                            "WHERE seller.Id = ?"
            );
            st.setInt(1, id);
            rs = st.executeQuery();
            if (rs.next()){
                Department dep = instantiateDepartment(rs);
                Seller obj = instantiateSeller(rs, dep);
                return obj;
            }
            return null;
        }
        catch (SQLException e){
            throw new DbException(e.getMessage());
        }
        finally {
            DB.closeStatement(st);
            DB.closeResultSet(rs);
        }
    }

    private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
        Seller obj = new Seller();
        obj.setId(rs.getInt("Id"));
        obj.setName(rs.getString("Name"));
        obj.setEmail(rs.getString("Email"));
        obj.setBaseSalary(rs.getDouble("BaseSalary"));
        obj.setBirthDate(rs.getDate("BirthDate"));
        obj.setDepartment(dep);
        return obj;
    }

    private Department instantiateDepartment(ResultSet rs) throws SQLException {
        Department dep = new Department();
        dep.setId(rs.getInt("DepartmentId"));
        dep.setName(rs.getString("DepName"));
        return dep;
    }

    @Override
    public List<Seller> findAll() {
            PreparedStatement st = null;
            ResultSet rs = null;

            try {
                st = conn.prepareStatement(
                        "SELECT seller.*, department.Name as DepName " +
                                "FROM seller INNER JOIN department " +
                                "ON seller.DepartmentId = department.Id " +
                                "ORDER BY Name"
                );
                rs = st.executeQuery();

                List<Seller> list = new ArrayList<>();
                Map<Integer, Department> map = new HashMap<>();

                while (rs.next()) {
                    Department dep = map.get(rs.getInt("DepartmentId"));

                    if (dep == null) {
                        dep = instantiateDepartment(rs);
                        map.put(rs.getInt("DepartmentId"), dep);
                    }

                    Seller obj = instantiateSeller(rs, dep);
                    list.add(obj);
                }
                return list;
            } catch (SQLException e) {
                throw new DbException(e.getMessage());
            } finally {
                DB.closeStatement(st);
                DB.closeResultSet(rs);
            }
    }

    @Override
    public List<Seller> findByDepartment(Department department) {
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            // Prepara a consulta SQL para buscar vendedores de um departamento específico
            st = conn.prepareStatement(
                    "SELECT seller.*, department.Name as DepName " + // Seleciona dados do vendedor e o nome do departamento
                            "FROM seller INNER JOIN department " +          // Faz um JOIN entre as tabelas seller e department
                            "ON seller.DepartmentId = department.Id " +     // Relaciona as tabelas pelo campo DepartmentId
                            "WHERE DepartmentId = ? " +                     // Filtra pelo ID do departamento
                            "ORDER BY Name"                                 // Ordena os resultados pelo nome do vendedor
            );
            st.setInt(1, department.getId()); // Define o parâmetro do ID do departamento na consulta
            rs = st.executeQuery(); // Executa a consulta e armazena o resultado no ResultSet

            List<Seller> list = new ArrayList<>(); // Lista para armazenar os vendedores encontrados
            Map<Integer, Department> map = new HashMap<>(); // Mapa para evitar instanciar departamentos duplicados

            while (rs.next()) { // Itera sobre as linhas do resultado da consulta
                // Verifica se o departamento já foi instanciado
                Department dep = map.get(rs.getInt("DepartmentId"));

                if (dep == null) { // Se o departamento ainda não foi instanciado
                    dep = instantiateDepartment(rs); // Instancia o departamento com os dados do ResultSet
                    map.put(rs.getInt("DepartmentId"), dep); // Armazena o departamento no mapa
                }

                // Instancia um vendedor com os dados do ResultSet e associa ao departamento
                Seller obj = instantiateSeller(rs, dep);
                list.add(obj); // Adiciona o vendedor à lista
            }
            return list; // Retorna a lista de vendedores
        } catch (SQLException e) {
            throw new DbException(e.getMessage()); // Lança uma exceção em caso de erro no SQL
        } finally {
            DB.closeStatement(st); // Fecha o PreparedStatement
            DB.closeResultSet(rs); // Fecha o ResultSet
        }
    }
}
