package cn.hashdata.bireme;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.Callable;

/**
 * 异常重试工具类
 *
 * @author L.cm
 */
public class RetryUtils {

    private static final Logger logger = LoggerFactory.getLogger(RetryUtils.class);

    /**
     * 回调结果检查
     */
    public interface ResultCheck {
        boolean matching();
        Connection getConnection();
    }

    /**
     * 在遇到异常时尝试重试
     *
     * @param retryLimit    重试次数
     * @param retryCallable 重试回调
     * @param <V>           泛型
     * @return V 结果
     */
    public static <V extends ResultCheck> V retryOnException(int retryLimit, java.util.concurrent.Callable<V> retryCallable) {

        V v = null;
        for (int i = 0; i < retryLimit; i++) {
            try {
                v = retryCallable.call();
                if (v.matching()) break;
            } catch (Exception e) {
                logger.warn("retry on " + (i + 1) + " times v = " + (v == null ? null : v.getConnection()), e);
            }
        }
        return v;
    }

    /**
     * 在遇到异常时尝试重试
     *
     * @param retryLimit    重试次数
     * @param sleepMillis   每次重试之后休眠的时间
     * @param retryCallable 重试回调
     * @param <V>           泛型
     * @return V 结果
     * @throws java.lang.InterruptedException 线程异常
     */
    public static <V extends ResultCheck> V retryOnException(int retryLimit, long sleepMillis,
                                                             java.util.concurrent.Callable<V> retryCallable) throws java.lang.InterruptedException {

        V v = null;
        for (int i = 0; i < retryLimit; i++) {
            try {
                v = retryCallable.call();
                if (v.matching()) {
                    break;
                }
            } catch (Exception e) {
                logger.info("retry on " + (i + 1) + " times v = " + (v == null ? null : v.getConnection()), e);
            }
            logger.info("retry on " + (i + 1) + " times v = " + (v == null ? null : v.getConnection()));
            Thread.sleep(sleepMillis);
        }
        return v;
    }

}
