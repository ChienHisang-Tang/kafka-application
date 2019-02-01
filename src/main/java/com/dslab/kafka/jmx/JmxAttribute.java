/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kafka.jmx;

import java.util.Optional;

/**
 *
 * @author 翔翔
 */

public interface JmxAttribute <T> {
    public Optional<T> getAttribute();
}
