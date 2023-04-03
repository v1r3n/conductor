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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.sdk.workflow.WorkflowMethod;
import com.netflix.conductor.sdk.workflow.def.ConductorWorkflow;
import com.netflix.conductor.sdk.workflow.utils.ObjectMapperProvider;

import static com.netflix.conductor.sdk.example.shipment.Order.ShippingMethod.SAME_DAY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class ShipmentWorkflow {

    private ShipmentWorkers2 worker;

    @WorkflowMethod(name = "order_flow")
    public void orderFlow(Order order) {

        Order calculated = worker.calculateTaxAndTotal(order.getCountryCode(), order.getQuantity(), order.getUnitPrice());
        User user = worker.getUserInfo(order.getUserId(), order.getQuantity());
        worker.sendEmail(calculated, user);
        User user2 = worker.getUserInfo(order.getUserId(), order.getQuantity());
        worker.sendEmail(calculated, user2);
        calculated = worker.calculateTaxAndTotal(order.getCountryCode(), order.getQuantity(), order.getUnitPrice());
        worker.sendEmail(calculated, user2);
        ConductorWorkflow workflow = ConductorWorkflow.current();


        //Option 1 --> inject variables as a map
//        workflow.transform(new Function<User, Address>() {
//            @Override
//            public Address apply(User o) {
//                return o.getAddress();
//            }
//        }, user);

//        Address address = workflow.transform(inputs -> {
//            User s = (User) inputs[0];
//            return s.getAddress();
//        }, user);

        Address address = (Address) workflow.transform(new Function<User, Address>() {
            @Override
            public Address apply(User o) {
                return null;
            }
        }, null);



        workflow.iff("$.user.address.city == 'NYC'?true:false",
                Map.of("user", user), worker.sendEmail(order, user)).elseif(worker.sendMail(new Order2(), new Address()));

        //Option 2 --> Allow inline code
        //Issue: this converts to a function
//        workflow.iff((userx) -> {
//            return false;
//        }, worker.sendEmail(order, user));
        //workflow.iff("user.address.city == 'NYC", worker.sendEmail(order, user), worker.sendEmail(order, user));

        workflow.iff("$.user.address.city == 'NYC'?true:false",
                Map.of("user", user), workflow.iff("true", Map.of("user", user), worker.sendEmail(order, user)));



    }

    public static void main(String[] args) throws JsonProcessingException {

        ObjectMapper om = new ObjectMapperProvider().getObjectMapper();
        ShipmentWorkflow shipment = ConductorWorkflow.newInstance(ShipmentWorkflow.class);
        ShipmentWorkers2 worker = ConductorWorkflow.newInstance(ShipmentWorkers2.class);
        ConductorWorkflow.current().startWorkers(ShipmentWorkers2.class.getPackageName());
        shipment.worker = worker;

        Order order = new Order();
        order.setOrderNumber("orderA");
        order.setCountryCode("US");
        order.setQuantity(12);
        order.setSku("S0847343");
        order.setShippingMethod(SAME_DAY);
        order.setUnitPrice(new BigDecimal("12.34"));
        order.setZipCode("10121");
        shipment.orderFlow(order);
        ConductorWorkflow workflow = ConductorWorkflow.current();
        workflow.executeDynamic(new HashMap<>());
        System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(workflow.toWorkflowDef()));
    }
    private boolean isDebugModeOn() {
        return true;
    }






}
