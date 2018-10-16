
#include <limits.h>
#include <stdint.h>
#include <string.h>

/* hadoop头文件 */
#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

using namespace std;

/* hadoop的mapper，reducer，和各自使用的context */
using HadoopPipes::TaskContext;
using HadoopPipes::Mapper;
using HadoopPipes::MapContext;
using HadoopPipes::Reducer;
using HadoopPipes::ReduceContext;

/* hadoop方法集中的两种方法 */
using HadoopUtils::toInt;
using HadoopUtils::toString;

/* hadoop运行入口 */
using HadoopPipes::TemplateFactory;
using HadoopPipes::runTask;

/* 公有继承hadoop的Mapper */
class LocalMapper : public Mapper
{
public:
	LocalMapper(TaskContext & context){}
	/* map函数，使用MapContext，将文件映射成键值对，键和值都是自己选的 */
	void map(MapContext & context)
	{
		/* 从文本中获取输入 */
		string line  = context.getInputValue();
        /* substr(a,b)意思是，从a指定的下标开始，取b个 */
		string key	 = line.substr(0, 1);
		string value = line.substr(2, 3);
		/* 根据筛选条件洗牌 */
		if (value != "100")
		{
			context.emit(key, value);
		}
	}
};

/* 公有继承Reducer */
class LocalReducer : public Reducer
{
public:
	LocalReducer(TaskContext & context){}
	/* reduce函数，使用ReduceContext */
	void reduce(ReduceContext & context)
	{
		int max_value = 0;
		/* 遍历一个key的所有value，根据筛选条件展示输出 */
		while (context.nextValue())
		{
			max_value = max(max_value, toInt(context.getInputValue()));
		}
		context.emit(context.getInputKey(), toString(max_value));
	}
};

int main()
{
	return runTask(TemplateFactory<LocalMapper, LocalReducer>());
}

