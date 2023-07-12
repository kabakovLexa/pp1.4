package jm.task.core.jdbc.dao;

import com.sun.xml.bind.v2.model.core.ID;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;

import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        Util util = new Util();
        Connection connection = util.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            String sql = "CREATE TABLE IF NOT EXISTS USER " +
                    "(id BIGINT PRIMARY KEY AUTO_INCREMENT," +
                    "name VARCHAR(255)," +
                    "lastName VARCHAR(255)," +
                    "age TINYINT)";

            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }


    }

    public void dropUsersTable() {
        Util util = new Util();
        Connection connection = util.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            String sql = "DROP TABLE IF EXISTS USER";
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    public void saveUser(String name, String lastName, byte age) {
        Util util = new Util();
        Connection connection = util.getConnection();
        PreparedStatement prepearedStatement = null;
        String sql = "INSERT INTO USER (name,lastName,age) VALUES (?,?,?)";

        try {
            prepearedStatement = connection.prepareStatement(sql);

            prepearedStatement.setString(1, name);
            prepearedStatement.setString(2, lastName);
            prepearedStatement.setByte(3, age);
            prepearedStatement.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    public void removeUserById(long id) {
        Util util = new Util();
        Connection connection = util.getConnection();
        String sql = "DELETE FROM USER WHERE id = ?";
        try {
            PreparedStatement prepareStatement = connection.prepareStatement(sql);
            prepareStatement.setLong(1, id);
            prepareStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        }


    }

    public List<User> getAllUsers() {
        Util util = new Util();
        Connection connection = util.getConnection();
        List<User> user = new ArrayList<>();
        String sql = "SELECT id,name,lastName,age FROM USER";
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                User user1 = new User();
                user1.setId(resultSet.getLong("id"));
                user1.setName(resultSet.getString("name"));
                user1.setLastName(resultSet.getString("lastName"));
                user1.setAge(resultSet.getByte("age"));
                user.add(user1);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return user;

    }

    public void cleanUsersTable() {
        Util util = new Util();
        Connection connection = util.getConnection();
        String sql = "TRUNCATE TABLE USER";
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }finally {
            if (connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        }



    }
}
