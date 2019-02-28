package cn.hashdata.bireme.config;

/**
 * 目标库链接信息
 */
public class TargetConnectionConfig {

    private String jdbcUrl;
    private String user;
    private String passwd;

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public TargetConnectionConfig setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getUser() {
        return user;
    }

    public TargetConnectionConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPasswd() {
        return passwd;
    }

    public TargetConnectionConfig setPasswd(String passwd) {
        this.passwd = passwd;
        return this;
    }
}