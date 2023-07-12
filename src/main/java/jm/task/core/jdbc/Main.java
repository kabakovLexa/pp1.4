package jm.task.core.jdbc;

import jm.task.core.jdbc.dao.UserDaoJDBCImpl;
import jm.task.core.jdbc.service.UserServiceImpl;
import jm.task.core.jdbc.util.Util;

public class Main {
    public static void main(String[] args) {
        // реализуйте алгоритм здесь


        UserServiceImpl userService = new UserServiceImpl();
        userService.createUsersTable();
        userService.saveUser("Алексей","Кабаков",(byte) 24);
        userService.saveUser("Роман","Степанов",(byte) 25);
        userService.saveUser("Екатерина","Кабаков",(byte) 17);
        userService.saveUser("София","Сакольцева",(byte) 23);
        System.out.println(userService.getAllUsers());
        userService.cleanUsersTable();
        userService.dropUsersTable();




    }
}
