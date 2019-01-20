<%--
  Created by IntelliJ IDEA.
  User: root
  Date: 2018/10/28
  Time: 20:00
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<html>
<head>
    <title>$Title$</title>
</head>
<body>
    <table border="1" align="center" width="50%">
        <tr>
            <th>编号</th>
            <th>姓名</th>
            <th>密码</th>
        </tr>
        <c:forEach items="${list}" var="vo">
            <tr>
                <td>${vo.id}</td>
                <td>${vo.name}</td>
                <td>${vo.password}</td>
            </tr>
        </c:forEach>
    </table>
</body>
</html>
