package cn.hashdata.bireme;

import java.sql.Connection;

/**
 * @author: : yangyang.li
 * Created time: 2018/11/27.
 * Copyright(c) TTPai All Rights Reserved.
 * Description :
 */
public class ConnectRetryEntry implements RetryUtils.ResultCheck {

    private Connection connection;

    public ConnectRetryEntry(Connection connection){
        this.connection = connection;
    }


    @Override
    public boolean matching() {
        return !(connection == null);
    }


    @Override
    public Connection getConnection() {
        return connection;
    }
}
