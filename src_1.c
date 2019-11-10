#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<pthread.h>
#include<time.h>

pthread_t ntid;

typedef struct
{
	int c_custkey;    	   //�˿ͱ��
	char c_mkgsegment[20]; //��Ӧ��ĳ���г�����
}customer;				   //�˿ͽṹ�� 

typedef struct
{
	int o_orderkey;    	 //������ 
	int o_custkey;    	 //�˿ͱ��
	char o_orderdate[10];//�������� 
}orders;				 //����

typedef struct
{
	int l_orderkey;//������
	double l_extendedprice;//����۸�
	char l_shipdate[10];//�������� 
}lineitem; //��Ʒ��Ϣ 

typedef struct
{
	int l_orderkey;//������
	char o_orderdate[10];//�������� 
	double l_extendedprice;//����۸�
}select_result;

typedef struct
{
	customer * cus ;//ָ��ͻ����ָ�� 
	orders * ord ;//ָ�򶩵����ָ�� 
	lineitem * item ;//ָ�� ��Ʒ���ָ�� 
	char order_date[20];
	char ship_date[20];
	char mktsegment[20];
	int limit; 
	//�Ӹ� semaphore;
	//cus,ord,item,order_date,ship_date,mktsegment
} ARG;

void printids(const char *s) {
	pid_t		pid;
	pthread_t	tid;

	pid = getpid();
	tid = pthread_self();
	printf("%s pid %lu tid %lu (0x%lx)\n", s, (unsigned long)pid,(unsigned long)tid, (unsigned long)tid);
}
//cus,ord,item,order_date,ship_date,mktsegment
void *thr_fn(void *arg) {
	ARG tmp = *(ARG*)arg;

	printids("new thread: ");
	printf("Change to C++ code!!");
	return((void *)0);
}


customer * read_customer_txt() //��ȡcustomer��txt���� 
{
	FILE * fp;
	customer *a=NULL;
	a = (customer *)malloc(100*sizeof(customer));
	int i=0;
	char b;
	fp = fopen("C:\\LIFE\\STUDY\\����ϵͳ\\ʵ��һ\\���Ӻ�4042017006\\customer.txt","r");
	if(fp==NULL)
	{
		printf("cannot open customer.txt!");
		return NULL;
	}
	while(!feof(fp))
	{	
//		printf("%d  ",i);
		fscanf(fp,"%d%c%s",&a[i].c_custkey,&b,&a[i].c_mkgsegment);
//		printf("%d%c%s\n",a[i].c_custkey,b,a[i].c_mkgsegment);
		i++;
	}
	fclose(fp);
	return a;
}
orders * read_orders_txt()//��ȡorders.txt���� 
{
	int i =0; 
	orders * a=NULL;
	a = (orders * )malloc(4000*sizeof(orders));
	char b,c;
	long long d;
	FILE *fp;
	fp = fopen("C:\\LIFE\\STUDY\\����ϵͳ\\ʵ��һ\\���Ӻ�4042017006\\orders.txt","r");
	if(fp == NULL)
	{
		printf("cannot open orders.txt!");
		return NULL;
	}
	while(!feof(fp))
	{	
		//printf("%d!",i);
		fscanf(fp,"%d%c%lld%c%s",&a[i].o_orderkey,&b,&d,&c,&a[i].o_orderdate);
		a[i].o_custkey=d%100;
//		printf("%d	%c	%lld	%c	%s\n",a[i].o_orderkey,b,a[i].o_custkey,c,a[i].o_orderdate);
		i++;
	}
	fclose(fp);
	return a;
}

lineitem * read_lineitem_txt()//��ȡlineitem.txt����
{
	FILE * fp;
	lineitem * l=NULL;
	l = (lineitem *)malloc(1000*sizeof(lineitem));
	int i=0;
	char b,c;
	fp = fopen("C:\\LIFE\\STUDY\\����ϵͳ\\ʵ��һ\\���Ӻ�4042017006\\lineitem.txt","r");
	if(fp==NULL)
	{
		printf("cannot open lineitem.txt!");
		return NULL;
	}
	while(!feof(fp))
	{
		//printf("%d!",i);
		fscanf(fp,"%d%c%lf%c%s",&l[i].l_orderkey,&c,&l[i].l_extendedprice,&b,&l[i].l_shipdate);
		//printf("%d,%lf,%s\n",l[i].l_orderkey,l[i].l_extendedprice,l[i].l_shipdate);
		i++;
	}
	fclose(fp);
	return l; 
}


void * Select(void *arg)//����ѡ�� 
{
	ARG tmp = *(ARG*)arg;
	double duration;
	clock_t start, finish;
	start=clock();
//	printf("mktsegment:%s\n",tmp.mktsegment);
	printf("mktsegment:%s	order_date:%s	ship_date:%s	limit:%d\n",tmp.mktsegment,tmp.order_date,tmp.ship_date,tmp.limit);
//	strcpy(tmp.mktsegment,"BUILDING");
	int i,j,k,l=0,m=0;
	select_result * result1=NULL;
	select_result * result2=NULL;
	select_result  temp;
	result1 = (select_result *)malloc(1001*sizeof(select_result));
	result2 = (select_result *)malloc(1001*sizeof(select_result));
	for(i=0;i<100;i++)
	{
		for(j=0;j<4000;j++)
		{
			for(k=0;k<1000;k++)
			if(tmp.cus[i].c_custkey== tmp.ord[j].o_custkey&&tmp.ord[j].o_orderkey== tmp.item[k].l_orderkey&&(strcmp(tmp.ord[j].o_orderdate, tmp.order_date)<0)&&(strcmp(tmp.item[k].l_shipdate, tmp.ship_date)>0)&&(strcmp(tmp.cus[i].c_mkgsegment, tmp.mktsegment)==0))
			{
//				printf("%d,%s,%lf\n",tmp.item[k].l_orderkey,tmp.ord[j].o_orderdate,tmp.item[k].l_extendedprice);
				result1[l].l_orderkey= tmp.item[k].l_orderkey;
				strcpy(result1[l].o_orderdate, tmp.ord[j].o_orderdate);
				result1[l].l_extendedprice= tmp.item[k].l_extendedprice;
				l++;
			}
		}
	}
	/*printf("���\n\n\n");*/
	for(i=0;i<l;i++)
	{
		//printf("%d\n",i);
		if(i==0)
		{
			result2[m].l_orderkey = result1[i].l_orderkey;
			strcpy(result2[m].o_orderdate,result1[i].o_orderdate);
			result2[m].l_extendedprice = result1[i].l_extendedprice;
			continue;
		}
		if(result1[i].l_orderkey==result1[i-1].l_orderkey)
		{
			result2[m].l_extendedprice = result2[m].l_extendedprice + result1[i].l_extendedprice;
			
		}
		else
		{
			m++;
			result2[m].l_orderkey = result1[i].l_orderkey;
			strcpy(result2[m].o_orderdate,result1[i].o_orderdate);
			result2[m].l_extendedprice = result1[i].l_extendedprice;
		}
	}/*
	for(i=0;i<=m;i++)
	{
		printf("%d,%s,%lf\n",result2[i].l_orderkey,result2[i].o_orderdate,result2[i].l_extendedprice);
	}*/
	for(i=0;i<m-1;i++)//ð������ ����ѡ������Ϊ���� 
	{
		for(j=0;j<m-1-i;j++)
		{
			//printf("%lf->%lf\n",result2[j].l_extendedprice,result2[j+1].l_extendedprice);
			if(result2[j].l_extendedprice<result2[j+1].l_extendedprice)
			{
				//printf("123");
				temp.l_extendedprice=result2[j].l_extendedprice;
				temp.l_orderkey=result2[j].l_orderkey;
				strcpy(temp.o_orderdate,result2[j].o_orderdate);
				result2[j].l_extendedprice=result2[j+1].l_extendedprice;
				result2[j].l_orderkey=result2[j+1].l_orderkey;
				strcpy(result2[j].o_orderdate,result2[j+1].o_orderdate);
				result2[j+1].l_extendedprice=temp.l_extendedprice;
				result2[j+1].l_orderkey=temp.l_orderkey;
				strcpy(result2[j+1].o_orderdate,temp.o_orderdate);
			}
		}
	}
	printf("l_orderkey|o_orderdate|revenue\n");
	for(j=0;j<tmp.limit;j++)
	{
		if(result2[j].l_extendedprice==0)
			printf("null      |null       |null   \n");
		else
//			printf("%-10d|%-11s|%-20.2lf\n",result[j].l_orderkey,result[j].o_orderdate,result[j].l_extendedprice);
			printf("%d|%s|%20.2lf\n", result2[j].l_orderkey, result2[j].o_orderdate, result2[j].l_extendedprice);
	}
	printids("main thread:");
	finish=clock();
	duration = (double)(finish - start) / CLOCKS_PER_SEC; 
	printf( "%f seconds\n", duration ); 
	/*
	for(i=0;i<=m;i++)
	{
		printf("/n%d,%s,%lf\n",result2[i].l_orderkey,result2[i].o_orderdate,result2[i].l_extendedprice);
	}*/
	return result2;//����ѡ������ָ�� 
}

int change_argv_to_number(char s[])//�������������������ַ���ת��Ϊ�������� 
{
	int i=0;
	int number=0;
	while(s[i]!='\0')
	{
		if(i==0)
			number = (s[i]-48);
		else
			number = number*10 + (s[i]-48);
		//printf("%d,%d\n",i,number);
		i++;
	}
	return number;
}

int main()//argc��ʾ�������ݵ��ܸ�����argv[]�ڱ�������������� 
{
	int i=0,j;
	double duration;
	clock_t start, finish;
	start=clock();
	int num;

	int limit;
	char order_date[15] = { 0 };
	char ship_date[10] = { 0 };
	char mktsegment[20] = { 0 };
//	select_result *iresult=NULL;
	customer * cus = NULL;//ָ��ͻ����ָ�� 
	orders * ord = NULL;//ָ�򶩵����ָ�� 
	lineitem * item = NULL;//ָ�� ��Ʒ���ָ�� 
	cus = read_customer_txt();//��ȡcustomer.txt������ ������ͻ��� 
	ord = read_orders_txt();//��ȡorders.txt������ �����붩���� 
	item = read_lineitem_txt();//��ȡlineitem.txt������ �������Ʒ�� 
	printf("������");
	scanf("%d",&num); 
	printf("���ݣ�");
	ARG arg[100];
	int result;

	while(num>0)
	{
		num--;
		scanf("%s %s %s %d",mktsegment,order_date,ship_date,&limit);
//		printf("%d	mktsegment:%s	order_date:%s	ship_date:%s	limit:%d\n",num,mktsegment,order_date,ship_date,limit);
		//��������ֵ��arg
		arg[i].cus = cus;
		arg[i].item = item;
		strcpy(arg[i].mktsegment,mktsegment);
		arg[i].ord = ord;
		strcpy(arg[i].order_date,order_date);
		strcpy(arg[i].ship_date,ship_date);
		arg[i].limit=limit;
		result = pthread_create(&ntid, NULL, (void *)&Select, &arg[i]);//��pthread�����������߳� 
		i++;

		sleep(1);
	}
	finish=clock();
	duration = (double)(finish - start) / CLOCKS_PER_SEC;
	printf( "%f seconds\n", duration );
	getchar();
	return 0;
}
