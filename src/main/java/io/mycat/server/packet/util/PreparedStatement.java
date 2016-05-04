/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.server.packet.util;

/* 表示一个逻辑的prepareStatement */
public class PreparedStatement {
    private long id;                /* ps id */
    private String statement;       /* sql string */
    private int columnsNumber;      /* 字段名个数 */
    private int parametersNumber;   /* ps中的参数个数 */
    private int[] parametersType;   /* ps中的参数类型 */


    public PreparedStatement(long id, String statement, int columnsNumber, int parametersNumber) {
        this.id = id;
        this.statement = statement;
        this.columnsNumber = columnsNumber;
        this.parametersNumber = parametersNumber;
        this.parametersType = new int[parametersNumber];
    }

    public long getId() { return id; }
    public String getStatement() { return statement; }
    public int getColumnsNumber() { return columnsNumber; }
    public int getParametersNumber() { return parametersNumber; }
    public int[] getParametersType() { return parametersType; }
}