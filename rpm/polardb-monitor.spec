BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}-root
Name: polardb-monitor
Version:0.1.0
Release: %(date +%%Y%%m%%d%%H%%M%%S)%{?dist}
License: ASL 2.0
Group: applications/polardb
Prefix: /opt/db-monitor
Summary: PolarDB Monitor

%description
Open Source PolarDB Monitor

%define __os_install_post %{nil}

%prep
rm -rf $RPM_BUILD_ROOT%{prefix}
cd $OLDPWD/../

%build
cd $OLDPWD/../
RPMNAME=%{name} RPMRELEASE=%{release} RPMVERSION=%{version} sh build.sh

%install
cd $OLDPWD/../
APP_BIN=$RPM_BUILD_ROOT%{prefix}/bin
APP_LIB=$RPM_BUILD_ROOT%{prefix}/lib
APP_CONF=$RPM_BUILD_ROOT%{prefix}/conf
APP_LOG=$RPM_BUILD_ROOT%{prefix}/log
mkdir -p ${APP_BIN} ${APP_LIB} ${APP_CONF} ${APP_LOG}
alias cp='cp'
pwd
cp -rf conf/* ${APP_CONF}
cp -rf bin/* ${APP_BIN}
cp scripts/service.sh ${APP_BIN}
sed -i 's/{BASEPATH}/\/opt\/db-monitor/g' ${APP_BIN}/service.sh

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-, root, root)
%attr(-, root, root) %{prefix}

%pre

%post

%preun

%changelog

