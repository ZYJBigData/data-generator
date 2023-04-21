package com.bizseer.bigdata.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementSetter {

    public void setValue(PreparedStatement preparedStatement) throws SQLException;

}
