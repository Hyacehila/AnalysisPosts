#!/usr/bin/env python3
"""
测试GLM模型调用功能
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.call_llm import call_glm_45_air, call_glm4v_plus, call_glm45v_thinking, call_glm46

def test_llm_models():
    """测试所有GLM模型"""
    print("GLM模型调用工具测试")
    print("=" * 50)
    
    # 检查测试图片是否存在
    image_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "test.png")
    image_exists = os.path.exists(image_path)
    
    try:
        # 测试1: glm-4.5-air纯文本模型
        print("\n=== 测试1: glm-4.5-air 纯文本模型 ===")
        text_response = call_glm_45_air("请简单介绍一下人工智能的发展历史。")
        print(f"文本模型响应: {text_response[:200]}...")
        
        # 测试2: glm4v-plus多模态模型（仅文本）
        print("\n=== 测试2: glm4v-plus 多模态模型（仅文本） ===")
        multimodal_response = call_glm4v_plus("请分析一下当前的技术发展趋势。")
        print(f"多模态模型响应: {multimodal_response[:200]}...")
        
        # 测试3: glm4v-plus视觉理解模型（带图片）
        if image_exists:
            print("\n=== 测试3: glm4v-plus 视觉理解模型（带图片） ===")
            vision_prompt = "请详细描述这张图片中的内容，包括主要对象、场景和可能的情感表达。"
            vision_response = call_glm4v_plus(vision_prompt, image_paths=[image_path])
            print(f"视觉理解模型响应: {vision_response[:300]}...")
        else:
            print(f"\n=== 测试3: glm4v-plus 视觉理解模型 ===")
            print(f"测试图片 {image_path} 不存在，跳过视觉测试")
        
        # 测试4: glm4.5v思考模式模型（纯文本）
        print("\n=== 测试4: glm4.5v 思考模式模型（纯文本） ===")
        thinking_response = call_glm45v_thinking("请分析人工智能在医疗领域的应用前景。", enable_thinking=True)
        print(f"思考模式模型响应: {thinking_response[:200]}...")
        
        # 测试5: glm4.5v思考模式模型（带图片）
        if image_exists:
            print("\n=== 测试5: glm4.5v 思考模式模型（带图片） ===")
            vision_thinking_prompt = "请详细描述这张图片中的内容，包括主要对象、场景和可能的情感表达。"
            vision_thinking_response = call_glm45v_thinking(
                vision_thinking_prompt, 
                image_paths=[image_path], 
                enable_thinking=True
            )
            print(f"视觉思考模式模型响应: {vision_thinking_response[:300]}...")
        else:
            print(f"\n=== 测试5: glm4.5v 思考模式模型（带图片） ===")
            print(f"测试图片 {image_path} 不存在，跳过视觉思考测试")
        
        # 测试6: glm4.6智能体推理模型
        print("\n=== 测试6: glm4.6 智能体推理模型 ===")
        reasoning_response = call_glm46("请设计一个舆情分析系统的架构方案。", enable_reasoning=True)
        print(f"推理模型响应: {reasoning_response[:200]}...")
        
        print("\n[SUCCESS] 所有测试通过！")
        return True
        
    except Exception as e:
        print(f"\n[FAILED] 测试失败: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_llm_models()
    sys.exit(0 if success else 1)
