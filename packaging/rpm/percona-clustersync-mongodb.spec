%undefine _missing_build_ids_terminate_build
%global debug_package %{nil}

Name:  percona-clustersync-mongodb
Version: @@VERSION@@
Release: @@RELEASE@@%{?dist}
Summary: Tool to sync data from one MongoDB cluster to another

Group:  Applications/Databases
License: ASL 2.0
Source0: percona-clustersync-mongodb-%{version}.tar.gz

BuildRequires: golang make git
BuildRequires:  systemd
BuildRequires:  pkgconfig(systemd)
Requires(pre): /usr/sbin/useradd, /usr/bin/getent
Requires(postun): /usr/sbin/userdel
Requires(post):   systemd
Requires(preun):  systemd
Requires(postun): systemd

%description
Percona ClusterSync for MongoDB is a tool for replicating data from a source MongoDB cluster to a target MongoDB cluster. It supports cloning data, replicating changes, and managing collections and indexes.

%prep
%setup -q -n percona-clustersync-mongodb-%{version}


%build
source ./VERSION
export VERSION
export GITBRANCH
export GITCOMMIT

cd ../
export PATH=/usr/local/go/bin:${PATH}
export GOROOT="/usr/local/go/"
export GOBINPATH="/usr/local/go/bin"
mkdir -p src/github.com/percona/
mv percona-clustersync-mongodb-%{version} src/github.com/percona/percona-clustersync-mongodb
ln -s src/github.com/percona/percona-clustersync-mongodb percona-clustersync-mongodb-%{version}
cd src/github.com/percona/percona-clustersync-mongodb
export GO111MODULE=on
export GOMODCACHE=$(pwd)/go-mod-cache
for i in {1..3}; do
    go mod tidy && go mod download && break
    echo "go mod commands failed, retrying in 10 seconds..."
    sleep 10
done
make build
cd %{_builddir}


%install
rm -rf $RPM_BUILD_ROOT
install -m 755 -d $RPM_BUILD_ROOT/%{_bindir}
cd ../
export PATH=/usr/local/go/bin:${PATH}
export GOROOT="/usr/local/go/"
export GOPATH=$(pwd)/
export PATH="/usr/local/go/bin:$PATH:$GOPATH"
export GOBINPATH="/usr/local/go/bin"
cd src/
cp github.com/percona/percona-clustersync-mongodb/bin/pcsm $RPM_BUILD_ROOT/%{_bindir}/pcsm
install -m 0755 -d $RPM_BUILD_ROOT/%{_sysconfdir}
install -m 0755 -d $RPM_BUILD_ROOT/%{_sysconfdir}/sysconfig
install -D -m 0640 github.com/percona/percona-clustersync-mongodb/packaging/conf/pcsm.env $RPM_BUILD_ROOT/%{_sysconfdir}/sysconfig/pcsm
install -m 0755 -d $RPM_BUILD_ROOT/%{_unitdir}
install -m 0644 github.com/percona/percona-clustersync-mongodb/packaging/conf/pcsm.service $RPM_BUILD_ROOT/%{_unitdir}/pcsm.service

%pre -n percona-clustersync-mongodb
	/usr/bin/getent group mongod || /usr/sbin/groupadd -r mongod
	/usr/bin/getent passwd mongod || /usr/sbin/useradd -M -r -g mongod -d /var/lib/mongo -s /bin/false -c mongod mongod
	if [ ! -f /var/log/pcsm.log ]; then
	    install -m 0640 -omongod -gmongod /dev/null /var/log/pcsm.log
	fi


%post -n percona-clustersync-mongodb
%systemd_post pcsm.service
if [ $1 == 1 ]; then
      /usr/bin/systemctl enable pcsm >/dev/null 2>&1 || :
fi

cat << EOF
** Join Percona Squad! **

Participate in monthly SWAG raffles, get early access to new product features,
invite-only ”ask me anything” sessions with database performance experts.

Interested? Fill in the form at https://squad.percona.com/mongodb

EOF


%postun -n percona-clustersync-mongodb
case "$1" in
   0) # This is a yum remove.
      %systemd_postun_with_restart pcsm.service
   ;;
esac


%files -n percona-clustersync-mongodb
%{_bindir}/pcsm
%config(noreplace) %attr(0640,root,root) /%{_sysconfdir}/sysconfig/pcsm
%{_unitdir}/pcsm.service


%changelog
* Wed Apr 16 2025 Surabhi Bhat <surbahi.bhat@percona.com>
- First Percona Link for MongoDB build
