package com.yuyuda.map.service;

import com.yuyuda.map.domain.ResultBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Service
public class StatService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<ResultBean> query() {
        String sql = "SELECT `longitude`, `latitude`, COUNT(1) `c` FROM `stat` GROUP BY `longitude`,`latitude`";
        return (List<ResultBean>) jdbcTemplate.query(sql, new RowMapper<ResultBean>() {
            @Override
            public ResultBean mapRow(ResultSet resultSet, int i) throws SQLException {
                ResultBean bean = new ResultBean();
                bean.setCount(resultSet.getLong("c"));
                bean.setLat(resultSet.getDouble("latitude"));
                bean.setLng(resultSet.getDouble("longitude"));

                return bean;
            }
        });
    }
}
