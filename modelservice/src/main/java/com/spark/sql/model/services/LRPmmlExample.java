package com.spark.sql.model.services;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yilong on 2019/8/25.
 */
public class LRPmmlExample {
    static class ModelParam {
        double step;
        double type;
        double amount;
        double oldBalanceOrig;
        double newBalanceOrig;
        double oldBalanceDest;
        double newBalanceDest;
        double errorBalanceOrig;
        double errorBalanceDest;
    }

    private Evaluator loadPmml(){
        PMML pmml = new PMML();
        InputStream inputStream = null;
        try {
            String fileUtl = this.getClass().getClassLoader().getResource("lrmodel.pkl.pmml").getPath();
            inputStream = new FileInputStream(fileUtl);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(inputStream == null){
            return null;
        }

        InputStream is = inputStream;

        try {
            pmml = org.jpmml.model.PMMLUtil.unmarshal(is);
        } catch (SAXException e1) {
            e1.printStackTrace();
        } catch (JAXBException e1) {
            e1.printStackTrace();
        }finally {
            //关闭输入流
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        Evaluator evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);

        pmml = null;
        return evaluator;
    }
    private int predict(Evaluator evaluator, ModelParam modelParam) {
        Map<String, Double> data = new HashMap<String, Double>();
        data.put("step", modelParam.step);
        data.put("type", modelParam.type);
        data.put("amount", modelParam.amount);
        data.put("oldBalanceOrig", modelParam.oldBalanceOrig);
        data.put("newBalanceOrig", modelParam.newBalanceOrig);
        data.put("oldBalanceDest", modelParam.oldBalanceDest);
        data.put("newBalanceDest", modelParam.newBalanceDest);
        data.put("errorBalanceOrig", modelParam.errorBalanceOrig);
        data.put("errorBalanceDest", modelParam.errorBalanceDest);

        List<InputField> inputFields = evaluator.getInputFields();

        //过模型的原始特征，从画像中获取数据，作为模型输入
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
        for (InputField inputField : inputFields) {
            FieldName inputFieldName = inputField.getName();
            Object rawValue = data.get(inputFieldName.getValue());
            FieldValue inputFieldValue = inputField.prepare(rawValue);
            arguments.put(inputFieldName, inputFieldValue);
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        List<TargetField> targetFields = evaluator.getTargetFields();

        TargetField targetField = targetFields.get(0);
        FieldName targetFieldName = targetField.getName();

        Object targetFieldValue = results.get(targetFieldName);
        System.out.println("target: " + targetFieldName.getValue() + " value: " + targetFieldValue);
        int primitiveValue = -1;
        if (targetFieldValue instanceof Computable) {
            Computable computable = (Computable) targetFieldValue;
            primitiveValue = (Integer)computable.getResult();
        }
        System.out.println( "primitiveValue : " + primitiveValue);
        return primitiveValue;
    }

    public static void main(String args[]){
        LRPmmlExample demo = new LRPmmlExample();
        Evaluator model = demo.loadPmml();
        ModelParam modelParam = new ModelParam();
        modelParam.step = 1;
        modelParam.amount = 8921;
        modelParam.oldBalanceOrig = 8900;
        modelParam.oldBalanceDest = 8935;
        modelParam.newBalanceOrig = 8931;
        modelParam.newBalanceDest = 8904;
        modelParam.errorBalanceOrig = -31;
        modelParam.errorBalanceDest = 31;

        demo.predict(model, modelParam);
    }
}
