
#include <limits.h> 
#include <stdint.h>  
#include <string>

#include <Pipes.hh>  
#include <TemplateFactory.hh> 
#include <StringUtils.hh>

using namespace std; 
 
/* 公有继承HadoopPipes::Mapper类 */ 
class MaxTemperatureMapper : public HadoopPipes::Mapper 
{
public:
	MaxTemperatureMapper(HadoopPipes::TaskContext & context){}
	/* map函数 */
	void map(HadoopPipes::MapContext & context)
	{
		/* 从文本中获取输入值，自动调用文本中每一行的操作应该是hadoop自动完成的 */
		string line			  = context.getInputValue();
		string year			  = line.substr(15, 4);
		string airTemperature = line.substr(87, 5);
		string q			  = line.substr(92, 1);

		if (airTemperature != "+9999" &&
			(q == "0" || q == "1" || q == "4" || q == "5" || q == "9"))
		{
			/* 指定键值对，前面是key，后面是value */ 
			context.emit(year, airTemperature);
		}
	}
};

class MapTemperatureReducer : public HadoopPipes::Reducer
{
public:
	MapTemperatureReducer(HadoopPipes::TaskContext & context){}
	/* reduce函数 */
	void reduce(HadoopPipes::ReduceContext & context)
	{
		int maxValue = INT_MIN;
		while (context.nextValue())
		{
			/* HadoopUtils提供的toInt方法，将map提供的输入值转换成int */
			/* 制作reduce规则，选择同样的key里面值最大的 */
			maxValue = max(maxValue, HadoopUtils::toInt(context.getInputValue()));
		}
		/* 产生结果，输出到文件 */
		context.emit(context.getInputKey(), HadoopUtils::toString(maxValue));
	}
};

int main(int argc, char *argv[])
{
	return HadoopPipes::runTask(HadoopPipes::TemplateFactory<MaxTemperatureMapper,
                              MapTemperatureReducer>());
}
