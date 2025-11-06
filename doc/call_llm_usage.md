# GLM模型调用工具使用文档

## 概述

`utils/call_llm.py` 提供了四个GLM模型的调用函数，使用zai-sdk作为调用package。

## 支持的模型

### 1. glm-4.5-air
- **类型**: 纯文本模型
- **特点**: 不开启推理模式，适用于基础文本处理任务
- **函数**: `call_glm_45_air()`

### 2. glm4v-plus
- **类型**: 视觉分析模型
- **特点**: 不开启推理模式，支持图像输入，适用于图像分析任务
- **函数**: `call_glm4v_plus()`

### 3. glm4.5v
- **类型**: 思考模式模型
- **特点**: 支持图像理解，开启思考模式，用于报告分析智能体
- **函数**: `call_glm45v_thinking()`

### 4. glm4.6
- **类型**: 智能体推理模型
- **特点**: 纯文本，开启推理模式，适用于复杂推理任务
- **函数**: `call_glm46()`

## 多模态推理模型组合

原本的多模态推理模型现在改为组合使用：
- **glm4.5v**: 负责视觉理解和思考模式
- **glm4.6**: 负责智能体推理

这种组合方式可以更好地发挥各自模型的优势。

## 函数详细说明

### call_glm_45_air()

```python
def call_glm_45_air(prompt: str, temperature: float = 0.7, max_tokens: Optional[int] = None) -> str
```

**参数**:
- `prompt` (str): 输入提示词
- `temperature` (float): 温度参数，控制随机性，默认0.7
- `max_tokens` (int, 可选): 最大生成token数

**使用示例**:
```python
from utils.call_llm import call_glm_45_air

result = call_glm_45_air(
    prompt="请分析这段文本的情感倾向",
    temperature=0.5,
    max_tokens=1000
)
print(result)
```

### call_glm4v_plus()

```python
def call_glm4v_plus(prompt: str, image_paths: Optional[List[str]] = None, 
                   image_data: Optional[List[bytes]] = None, 
                   temperature: float = 0.7, max_tokens: Optional[int] = None) -> str
```

**参数**:
- `prompt` (str): 输入提示词
- `image_paths` (List[str], 可选): 图片文件路径列表
- `image_data` (List[bytes], 可选): 图片二进制数据列表（与image_paths二选一）
- `temperature` (float): 温度参数，控制随机性，默认0.7
- `max_tokens` (int, 可选): 最大生成token数

**使用示例**:
```python
from utils.call_llm import call_glm4v_plus

# 使用图片路径
result = call_glm4v_plus(
    prompt="请分析这张图片的内容",
    image_paths=["image1.jpg", "image2.png"],
    temperature=0.7
)

# 使用图片二进制数据
with open("image.jpg", "rb") as f:
    img_data = f.read()

result = call_glm4v_plus(
    prompt="请分析这张图片的内容",
    image_data=[img_data],
    temperature=0.7
)
```

### call_glm45v_thinking()

```python
def call_glm45v_thinking(prompt: str, image_paths: Optional[List[str]] = None, 
                        image_data: Optional[List[bytes]] = None, 
                        temperature: float = 0.7, max_tokens: Optional[int] = None, 
                        enable_thinking: bool = True) -> str
```

**参数**:
- `prompt` (str): 输入提示词
- `image_paths` (List[str], 可选): 图片文件路径列表
- `image_data` (List[bytes], 可选): 图片二进制数据列表（与image_paths二选一）
- `temperature` (float): 温度参数，控制随机性，默认0.7
- `max_tokens` (int, 可选): 最大生成token数
- `enable_thinking` (bool): 是否开启思考模式，默认True

**使用示例**:
```python
from utils.call_llm import call_glm45v_thinking

# 纯文本思考模式
result = call_glm45v_thinking(
    prompt="请深度分析这个复杂的业务场景",
    temperature=0.8,
    enable_thinking=True
)
print(result)

# 带图片的视觉理解思考模式
result = call_glm45v_thinking(
    prompt="请深度分析这张图片中的舆情信息，包括情感倾向、主题内容和潜在影响",
    image_paths=["image1.jpg", "image2.png"],
    temperature=0.8,
    enable_thinking=True
)
print(result)

# 使用图片二进制数据
with open("image.jpg", "rb") as f:
    img_data = f.read()

result = call_glm45v_thinking(
    prompt="请分析这张图片中的情感表达和内容特征",
    image_data=[img_data],
    temperature=0.8,
    enable_thinking=True
)
print(result)
```

### call_glm46()

```python
def call_glm46(prompt: str, temperature: float = 0.8, 
               max_tokens: Optional[int] = None, enable_reasoning: bool = True) -> str
```

**参数**:
- `prompt` (str): 输入提示词
- `temperature` (float): 温度参数，控制随机性，默认0.8
- `max_tokens` (int, 可选): 最大生成token数
- `enable_reasoning` (bool): 是否开启推理模式，默认True

**使用示例**:
```python
from utils.call_llm import call_glm46

result = call_glm46(
    prompt="请设计一个完整的舆情分析系统架构",
    temperature=0.8,
    enable_reasoning=True
)
print(result)
```

## 配置说明

### API密钥配置

API密钥在代码中硬编码配置：

```python
GLM_API_KEY = "your_api_key_here"  # 请替换为实际的API密钥
```

使用前请将 `your_api_key_here` 替换为实际的API密钥。

### 依赖包

- `zai`: GLM API的Python SDK
- `base64`: 图片编码
- `typing`: 类型注解

## 错误处理

所有函数都包含完善的错误处理：

- API密钥未配置时会抛出异常
- 网络连接问题会抛出异常
- 模型调用失败会抛出异常
- 异常信息包含具体的错误原因

**建议的错误处理**:
```python
try:
    result = call_glm_45_air("测试提示词")
    print(result)
except Exception as e:
    print(f"调用失败: {e}")
```

## 测试

运行测试函数：

```bash
python utils/call_llm.py
```

测试函数会依次测试四个模型的基本功能。

## 最佳实践

1. **模型选择**:
   - 基础文本处理：使用 `glm-4.5-air-x`
   - 图像分析：使用 `glm4v-plus`
   - 复杂视觉理解：使用 `glm4.5v`
   - 复杂推理任务：使用 `glm4.6`

2. **参数调优**:
   - 创造性任务：使用较高的temperature (0.8-1.0)
   - 分析性任务：使用较低的temperature (0.3-0.7)
   - 长文本生成：设置合适的max_tokens限制

3. **多模态组合**:
   - 先用 `glm4.5v` 理解图像内容
   - 再用 `glm4.6` 进行深度推理分析

4. **性能考虑**:
   - 避免过长的输入文本
   - 合理设置max_tokens避免资源浪费
   - 批量处理时考虑并发调用

## 注意事项

1. API密钥已硬编码在代码中，请确保安全性
2. 图片支持JPEG和PNG格式
3. 所有函数都是同步调用
4. 网络异常时会有重试机制（由zai-sdk处理）
5. 建议在生产环境中添加日志记录
