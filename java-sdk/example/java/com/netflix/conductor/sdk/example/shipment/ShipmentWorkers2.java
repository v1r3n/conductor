/*
 * Copyright 2022 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.sdk.example.shipment;

import com.netflix.conductor.sdk.workflow.task.InputParam;
import com.netflix.conductor.sdk.workflow.task.WorkerTask;

import java.math.BigDecimal;

public class ShipmentWorkers2 implements ShipmentWorkerInterface {

    //@WorkerTask("calculate_tax_and_total")
    public Order calculateTaxAndTotal(@InputParam("countryCode") String countryCode,
                                      @InputParam("quantity") int quantity, @InputParam("unitPrice") BigDecimal unitPrice) {
        Order order = new Order();
        order.setCountryCode(countryCode);
        order.setQuantity(quantity);
        order.setUnitPrice(unitPrice);
        order.setUserId(123);
        return order;
    }

    @WorkerTask("ground_shipping")
    public Void groundShipping(User user, String orderNum) {
        return null;
    }

    @WorkerTask("send_email")
    public double sendEmail(@InputParam("order") Order order, @InputParam("user") User user) {
        return 2.34d;
    }

    @WorkerTask("send_mail")
    public double sendMail(@InputParam("order") Order2 order, Address user, double charge) {
        return 10.0d;
    }

    @WorkerTask("get_user_info")
    public User getUserInfo(@InputParam("userId") int userId, @InputParam("orderQuantity") int orderQuantity) {
        User user = new User();
        user.setCity("NYC");
        user.setId(345);
        user.setName("Orkes User");
        Address address = new Address();
        address.setBlock("blockA");
        address.setCity("NYC");
        address.setCountry("US");
        address.setStreet("1000 orkes lane");
        address.setZip("10121");
        user.setAddress(address);
        return user;
    }
}
