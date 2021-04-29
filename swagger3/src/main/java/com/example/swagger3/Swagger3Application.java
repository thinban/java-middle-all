package com.example.swagger3;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;
import lombok.Data;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.oas.annotations.EnableOpenApi;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;


@Api(tags = "用户信息管理")
@RestController
@EnableOpenApi
@SpringBootApplication
public class Swagger3Application {

    public static void main(String[] args) {
        SpringApplication.run(Swagger3Application.class, args);
    }

    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.OAS_30)
                .apiInfo(new ApiInfoBuilder()
                        .title("Swagger3接口文档")
                        .description("Swagger3接口文档。")
                        .contact(new Contact("thinban", "thinban", "thinban"))
                        .version("1.0")
                        .build())
                .select()
                .apis(RequestHandlerSelectors.withMethodAnnotation(ApiOperation.class))
                .paths(PathSelectors.any())
                .build();
    }

    @Data
    @ApiModel
    public static class Param {
        @ApiModelProperty(value = "姓名", required = true, example = "thinban")
        private String name;
    }

    @Data
    @ApiModel
    public static class ParamRes {
        @ApiModelProperty(value = "姓名", required = true, example = "thinban")
        private String name;
    }


    @PostMapping("/test")
    @ApiOperation(value = "测试访问", notes = "传入name", response = ParamRes.class)
    public ParamRes test(@RequestBody Param param) {
        ParamRes paramRes = new ParamRes();
        BeanUtils.copyProperties(param, paramRes);
        return paramRes;
    }
}
