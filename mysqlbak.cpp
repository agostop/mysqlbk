#include <string>
#include <iostream>
#include <stdlib>

std::string join(char *str[], int count, std::string &split_char);

int main(int argc, char *argv[])
{
        if (argc == 1)
        {
                std::cout << "not have param"<< std::endl;
                exit(1);
        }
        std::string split(" ");
        std::string args(join(argv,argc,split));

        std::string cml("/usr/local/mysql/bin/mysqldump -uroot -p\"PASSWORD\"");
        cml.append(" "+args);
        system(cml.c_str());
        return 0;
}

std::string join(char *str[], int count, std::string &split_char)
{
	std::string new_str;
	for (int i=1;i!=count;++i)
	{
		new_str.append(str[i]);
		if ((i+1)==count)
			break;
		new_str.append(split_char);
	}
	return new_str;
}
