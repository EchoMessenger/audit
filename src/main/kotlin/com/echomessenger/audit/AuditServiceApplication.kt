package com.echomessenger.audit

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.runApplication
import org.springframework.cache.annotation.EnableCaching
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication(exclude = [DataSourceAutoConfiguration::class])
@EnableCaching
@EnableScheduling
class AuditServiceApplication

fun main(args: Array<String>) {
    runApplication<AuditServiceApplication>(*args)
}
