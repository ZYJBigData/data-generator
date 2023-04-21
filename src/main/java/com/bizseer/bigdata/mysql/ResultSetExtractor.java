package com.bizseer.bigdata.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetExtractor<R> {

    public R extract(ResultSet resultSet) throws SQLException;

}