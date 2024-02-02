package com.bizseer.bigdata.mysql;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class ConnectionHolder {

    private Connection connection;

    private Set<PreparedStatement> preparedStatements = new HashSet<>();

    private Set<ResultSet> resultSets = new HashSet<>();

    public ConnectionHolder(Connection connection) {
        this.connection = connection;
    }

    public PreparedStatement createPreparedStatement(String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        preparedStatements.add(statement);
        return statement;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql,null);
    }

    public ResultSet executeQuery(String sql, PreparedStatementSetter statementSetter) throws SQLException {
        PreparedStatement preparedStatement = this.createPreparedStatement(sql);
        if(null!=statementSetter){
            statementSetter.setValue(preparedStatement);
        }
        ResultSet resultSet = preparedStatement.executeQuery();
        this.resultSets.add(resultSet);
        return resultSet;
    }

    public int executeUpdate(String sql, Consumer<PreparedStatement> statementSetter) throws SQLException {
        PreparedStatement preparedStatement = this.createPreparedStatement(sql);
        if(null!=statementSetter){
            statementSetter.accept(preparedStatement);
        }
        return preparedStatement.executeUpdate();
    }

    public void close() throws SQLException {
        for(ResultSet resultSet : resultSets){
            if(null!=resultSet){
                resultSet.close();
            }
        }
        resultSets.clear();
        for(PreparedStatement statement : preparedStatements){
            if(null != statement){
                statement.close();
            }
        }
        preparedStatements.clear();
        if(null!=connection){
            connection.close();
        }
        connection = null;

    }
}
