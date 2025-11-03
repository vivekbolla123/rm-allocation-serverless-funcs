//package com.akasaair.queues.common.config;
//
//import org.hibernate.jpa.HibernatePersistenceProvider;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.autoconfigure.orm.jpa.JpaProperties;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.orm.jpa.JpaTransactionManager;
//import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
//import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
//import org.springframework.transaction.PlatformTransactionManager;
//
//import javax.sql.DataSource;
//import java.util.HashMap;
//import java.util.Map;
//
//import com.zaxxer.hikari.HikariDataSource;
//
//@Configuration
//@EnableJpaRepositories(
//        basePackages = "com.akasaair.nav_ods_webservices.file_generation",
//        entityManagerFactoryRef = "rmallocEntityManagerFactory",
//        transactionManagerRef = "rmallocTransactionManager"
//)
//public class RmAllocConfig {
//
//    @Autowired
//    private JpaProperties jpaProperties;
//
//    @Value("${rm.db.url}")
//    private String url;
//
//    @Value("${rm.db.username}")
//    private String username;
//
//    @Value("${RM_DB_APPUSER_PASSWORD}")
//    private String password;
//
//    @Bean(name = "writeDataSource")
//    public DataSource writeDataSource() {
//        String driverClassName = "com.mysql.cj.jdbc.Driver";
//
//        HikariDataSource dataSource = new HikariDataSource();
//        dataSource.setJdbcUrl(url);
//        dataSource.setUsername(username);
//        dataSource.setPassword(password);
//        dataSource.setDriverClassName(driverClassName);
//        dataSource.setMaximumPoolSize(10);
//        dataSource.setMinimumIdle(2);
//        dataSource.setIdleTimeout(300000);
//        dataSource.setConnectionTimeout(30000);
//        dataSource.setPoolName("WritePool");
//
//        return dataSource;
//    }
//
//    @Bean(name = "writeJdbcTemplate")
//    public JdbcTemplate writeJdbcTemplate(@Qualifier("writeDataSource") DataSource writeDataSource) {
//        return new JdbcTemplate(writeDataSource);
//    }
//
//    @Bean
//    public LocalContainerEntityManagerFactoryBean rmallocEntityManagerFactory() {
//        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
//        em.setDataSource(writeDataSource());
//        em.setPackagesToScan("com.akasaair.nav_ods_webservices.file_generation");
//        em.setPersistenceProviderClass(HibernatePersistenceProvider.class);
//        em.setPersistenceUnitName("rmAllocPUP");
//
//        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
//        em.setJpaVendorAdapter(vendorAdapter);
//
//        Map<String, Object> properties = new HashMap<>(jpaProperties.getProperties());
//        // Optional Hibernate overrides
//        properties.putIfAbsent("hibernate.hbm2ddl.auto", "none");
//        properties.putIfAbsent("hibernate.dialect", "org.hibernate.dialect.MySQL8Dialect");
//
//        em.setJpaPropertyMap(properties);
//
//        return em;
//    }
//
//    @Bean
//    public PlatformTransactionManager rmallocTransactionManager() {
//        JpaTransactionManager transactionManager = new JpaTransactionManager();
//        transactionManager.setEntityManagerFactory(rmallocEntityManagerFactory().getObject());
//        return transactionManager;
//    }
//}
