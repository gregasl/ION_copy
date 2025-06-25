import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 主函数 - 程序入口
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("========================================");
        LOGGER.info("订单监控系统 v4.0 (独立版)");
        LOGGER.info("========================================");
        
        OrderMonitor monitor = new OrderMonitor();
        
        // 先启动监控
        LOGGER.info("启动订单监控器...");
        monitor.start();
        
        // 等待监控器完全启动
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            LOGGER.error("等待监控器启动时被中断", e);
            return;
        }
        
        // 然后进行测试
        LOGGER.info("测试ION连接...");
        monitor.testIONConnection();
        
        // 保持程序运行
        LOGGER.info("系统运行中... 按 Ctrl+C 停止");
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.info("程序被用户中断");
        }
    }
}