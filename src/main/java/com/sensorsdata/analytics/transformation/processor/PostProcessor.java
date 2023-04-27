package com.sensorsdata.analytics.transformation.processor;

import com.sensorsdata.analytics.bean.format.SensorsEventBean;

@FunctionalInterface
public interface PostProcessor {

    /**
     * 用于在处理流程的最后进行 isSignUpEvent 的判断
     *
     * @param sensorsEvent - 处理完成后的神策数据格式 Bean
     * @return 这个 Bean 内的数据是否为登录事件
     */
    boolean isSignUpEvent(SensorsEventBean sensorsEvent);
}
