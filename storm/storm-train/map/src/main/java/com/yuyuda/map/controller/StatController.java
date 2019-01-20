package com.yuyuda.map.controller;

import com.alibaba.druid.support.json.JSONUtils;
import com.yuyuda.map.domain.ResultBean;
import com.yuyuda.map.service.StatService;
import net.sf.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;

@Controller
public class StatController {

    @Autowired
    private StatService statService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public ModelAndView map() {
        ModelAndView view = new ModelAndView("map");
        List<ResultBean> list = statService.query();
        JSONArray jsonArray = JSONArray.fromObject(list);
        System.out.println(jsonArray);
        view.addObject("res", jsonArray);
        return view;
    }




}
