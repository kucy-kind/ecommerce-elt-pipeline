CREATE TABLE dwd_dim_merchants ( #新加的表
    merchant_id VARCHAR(50) PRIMARY KEY,
    merchant_name VARCHAR(100) NOT NULL,
    register_time DATETIME NOT NULL,
    merchant_status ENUM('active', 'inactive', 'banned') DEFAULT 'active',
    contact_info VARCHAR(200),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_status (merchant_status),
    INDEX idx_register_time (register_time)
);

CREATE TABLE dwd_users ( #不变
    user_id VARCHAR(50) PRIMARY KEY,
    user_name VARCHAR(50) NOT NULL DEFAULT '游客',
    us_address TEXT,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_users (user_id)
);
CREATE TABLE dwd_dim_promotions ( #不变
    promotion_id VARCHAR(50) PRIMARY KEY,
    promotion_name VARCHAR(200) NOT NULL,
    promotion_type ENUM('Full reduction', 'Discount', 'Coupon', 'Limited-Time Sale', 'Bundle Deal') NOT NULL,
    applicable_categories JSON,
    discount_rules JSON NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    promotion_status ENUM('Not Started', 'In Progress', 'Completed', 'Cancelled') NOT NULL,
    target_audience VARCHAR(100),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_type (promotion_type),
    INDEX idx_status (promotion_status),
    INDEX idx_time_range (start_time, end_time)
);



CREATE TABLE dwd_dim_products ( #加了商家外键
    product_id VARCHAR(50) PRIMARY KEY,
    merchant_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    product_category_id VARCHAR(50) NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    listing_time DATETIME NOT NULL,
    delisting_time DATETIME,
    current_stock INT NOT NULL DEFAULT 0,
    product_status ENUM('active', 'inactive', 'out_of_stock', 'banned') NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    average_rating DECIMAL(3,2),
    rating_count INT DEFAULT 0,
    product_lifecycle ENUM('new', 'hot', 'normal', 'declining', 'discontinued') DEFAULT 'normal',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (merchant_id) REFERENCES dwd_dim_merchants(merchant_id),
    INDEX idx_merchant_id (merchant_id),
    INDEX idx_category (product_category_id),
    INDEX idx_status (product_status),
    INDEX idx_listing_time (listing_time)
);

CREATE TABLE dwd_orders ( #合并了时间段
    order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    order_amount DECIMAL(10,2) NOT NULL,
    order_status ENUM('pending', 'paid', 'shipped', 'completed', 'cancelled', 'refunded') NOT NULL,
    shipping_address TEXT NOT NULL,
    promotion_id VARCHAR(50),
    coupon_info JSON,
    data_quality_flag ENUM('valid', 'invalid', 'suspicious') DEFAULT 'valid',
    -- 合并的时间字段
    order_time DATETIME NOT NULL,          -- 下单时间
    payment_time DATETIME,                 -- 支付时间
    shipping_time DATETIME,                 -- 发货时间
    complete_time DATETIME,                 -- 完成时间
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES dwd_users(user_id),
    FOREIGN KEY (product_id) REFERENCES dwd_dim_products(product_id),
    FOREIGN KEY (promotion_id) REFERENCES dwd_dim_promotions(promotion_id),

    INDEX idx_user_id (user_id),
    INDEX idx_product_id (product_id),
    INDEX idx_status (order_status),
    INDEX idx_order_time (order_time)
);

CREATE TABLE dwd_user_actions ( #加了商品外键
    action_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(100),
    action_type ENUM('register', 'login', 'browse', 'favorite', 'add_cart', 'purchase', 'search', 'logout') NOT NULL,
    action_time DATETIME NOT NULL,
    product_id VARCHAR(50),
    stay_duration_seconds INT,
    device_type VARCHAR(50),
    is_abnormal BOOLEAN DEFAULT FALSE,
    abnormal_reason VARCHAR(200),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES dwd_users(user_id),
    FOREIGN KEY (product_id) REFERENCES dwd_dim_products(product_id),

    INDEX idx_user_id (user_id),
    INDEX idx_action_time (action_time),
    INDEX idx_session_id (session_id),
    INDEX idx_action_type (action_type)
);

CREATE TABLE dwd_returns (#合并时间字段
    return_id VARCHAR(50) PRIMARY KEY,
    order_id BIGINT NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    merchant_id VARCHAR(50) NOT NULL,
    return_reason_category ENUM('quality_issue', 'wrong_item', 'damaged', 'size_issue', 'not_as_described', 'other') NOT NULL,
    return_reason_detail TEXT,
    return_status ENUM('applied', 'approved', 'rejected', 'shipped_back', 'received', 'refunded') NOT NULL,
    -- 合并的时间字段
    apply_time DATETIME NOT NULL,
    approved_time DATETIME,
    rejected_time DATETIME,
    shipped_back_time DATETIME,
    received_time DATETIME,
    refund_time DATETIME,
    refund_amount DECIMAL(10,2),
    return_quantity INT NOT NULL,
    data_consistency_flag ENUM('consistent', 'inconsistent') DEFAULT 'consistent',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (order_id) REFERENCES dwd_orders(order_id),
    FOREIGN KEY (user_id) REFERENCES dwd_users(user_id),
    FOREIGN KEY (merchant_id) REFERENCES dwd_dim_merchants(merchant_id),

    INDEX idx_order_id (order_id),
    INDEX idx_user_id (user_id),
    INDEX idx_merchant_id (merchant_id),
    INDEX idx_apply_time (apply_time),
    INDEX idx_return_status (return_status)
);

CREATE TABLE dwd_merchant_operations (#移除冗余字段，增加外键
    operation_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    merchant_id VARCHAR(50) NOT NULL,
    operation_type ENUM('add_product', 'remove_product', 'update_price', 'update_stock') NOT NULL,
    new_place DECIMAL(10,2),
    product_id VARCHAR(50) NOT NULL,
    operation_time DATETIME NOT NULL,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (merchant_id) REFERENCES dwd_dim_merchants(merchant_id),
    FOREIGN KEY (product_id) REFERENCES dwd_dim_products(product_id),

    INDEX idx_merchant_id (merchant_id),
    INDEX idx_operation_type (operation_type),
    INDEX idx_operation_time (operation_time)
);

CREATE TABLE dwd_data_quality_alerts (#不变
    alert_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    check_type VARCHAR(50) NOT NULL,
    problem_description TEXT NOT NULL,
    problem_count INT NOT NULL,
    severity ENUM('low', 'medium', 'high', 'critical') DEFAULT 'medium',
    check_time DATETIME NOT NULL,
    data_date DATE NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT,
    resolved_time DATETIME,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_check_type (check_type),
    INDEX idx_data_date (data_date),
    INDEX idx_severity (severity),
    INDEX idx_resolved (resolved)
);

DELIMITER $$

CREATE TRIGGER trg_orders_before_insert
BEFORE INSERT ON dwd_orders
FOR EACH ROW
BEGIN
    -- 如果应用程序未提供 order_time，则设置为当前时间（通常应有值）
    IF NEW.order_time IS NULL THEN
        SET NEW.order_time = NOW();
    END IF;
    -- 根据初始状态设置对应时间（如有需要）
    IF NEW.order_status = 'paid' THEN
        SET NEW.payment_time = NOW();
    ELSEIF NEW.order_status = 'shipped' THEN
        SET NEW.shipping_time = NOW();
    ELSEIF NEW.order_status = 'completed' THEN
        SET NEW.complete_time = NOW();
    END IF;
END$$

CREATE TRIGGER trg_orders_before_update
BEFORE UPDATE ON dwd_orders
FOR EACH ROW
BEGIN
    -- 当订单状态改变时，更新对应的时间字段
    IF NEW.order_status <> OLD.order_status THEN
        IF NEW.order_status = 'paid' AND OLD.order_status <> 'paid' THEN
            SET NEW.payment_time = NOW();
        ELSEIF NEW.order_status = 'shipped' AND OLD.order_status <> 'shipped' THEN
            SET NEW.shipping_time = NOW();
        ELSEIF NEW.order_status = 'completed' AND OLD.order_status <> 'completed' THEN
            SET NEW.complete_time = NOW();
        END IF;
    END IF;
END$$

DELIMITER ;

DELIMITER $$

CREATE TRIGGER trg_returns_before_insert
BEFORE INSERT ON dwd_returns
FOR EACH ROW
BEGIN
    -- 如果应用程序未提供 apply_time，则设置为当前时间
    IF NEW.apply_time IS NULL THEN
        SET NEW.apply_time = NOW();
    END IF;
    -- 根据初始状态设置其他时间（如有需要）
    IF NEW.return_status = 'approved' THEN
        SET NEW.approved_time = NOW();
    ELSEIF NEW.return_status = 'rejected' THEN
        SET NEW.rejected_time = NOW();
    ELSEIF NEW.return_status = 'shipped_back' THEN
        SET NEW.shipped_back_time = NOW();
    ELSEIF NEW.return_status = 'received' THEN
        SET NEW.received_time = NOW();
    ELSEIF NEW.return_status = 'refunded' THEN
        SET NEW.refund_time = NOW();
    END IF;
END$$

CREATE TRIGGER trg_returns_before_update
BEFORE UPDATE ON dwd_returns
FOR EACH ROW
BEGIN
    -- 当退货状态改变时，更新对应的时间字段
    IF NEW.return_status <> OLD.return_status THEN
        IF NEW.return_status = 'approved' AND OLD.return_status NOT IN ('approved', 'rejected') THEN
            SET NEW.approved_time = NOW();
        ELSEIF NEW.return_status = 'rejected' AND OLD.return_status <> 'rejected' THEN
            SET NEW.rejected_time = NOW();
        ELSEIF NEW.return_status = 'shipped_back' AND OLD.return_status <> 'shipped_back' THEN
            SET NEW.shipped_back_time = NOW();
        ELSEIF NEW.return_status = 'received' AND OLD.return_status <> 'received' THEN
            SET NEW.received_time = NOW();
        ELSEIF NEW.return_status = 'refunded' AND OLD.return_status <> 'refunded' THEN
            SET NEW.refund_time = NOW();
        END IF;
    END IF;
END$$

DELIMITER ; 