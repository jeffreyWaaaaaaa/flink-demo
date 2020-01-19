package com.jw9j.flink.etltest;

import jdk.nashorn.internal.objects.annotations.Getter;
import jdk.nashorn.internal.objects.annotations.Setter;


public class OrderDetail {
    private String orderId;
    private String orderDetailId;
    private String memberId;
    private int childOrderSource;
    private String productColor;
    private String productName;
    private String productTitle;
    private String orderAmount;
    private String originalPrice;
    private int quantity;
    private float amountPayable;
    private String size;

    public OrderDetail(String orderId, String orderDetailId, String memberId, int childOrderSource, String productColor, String productName, String productTitle, String orderAmount, String originalPrice, int quantity, float amountPayable, String size) {
        this.orderId = orderId;
        this.orderDetailId = orderDetailId;
        this.memberId = memberId;
        this.childOrderSource = childOrderSource;
        this.productColor = productColor;
        this.productName = productName;
        this.productTitle = productTitle;
        this.orderAmount = orderAmount;
        this.originalPrice = originalPrice;
        this.quantity = quantity;
        this.amountPayable = amountPayable;
        this.size = size;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderDetailId() {
        return orderDetailId;
    }

    public void setOrderDetailId(String orderDetailId) {
        this.orderDetailId = orderDetailId;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public int getChildOrderSource() {
        return childOrderSource;
    }

    public void setChildOrderSource(int childOrderSource) {
        this.childOrderSource = childOrderSource;
    }

    public String getProductColor() {
        return productColor;
    }

    public void setProductColor(String productColor) {
        this.productColor = productColor;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductTitle() {
        return productTitle;
    }

    public void setProductTitle(String productTitle) {
        this.productTitle = productTitle;
    }

    public String getOrderAmount() {
        return orderAmount;
    }

    public void setOrderAmount(String orderAmount) {
        this.orderAmount = orderAmount;
    }

    public String getOriginalPrice() {
        return originalPrice;
    }

    public void setOriginalPrice(String originalPrice) {
        this.originalPrice = originalPrice;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public float getAmountPayable() {
        return amountPayable;
    }

    public void setAmountPayable(float amountPayable) {
        this.amountPayable = amountPayable;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }
}
