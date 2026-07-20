# IPTVs App (Fixed)

Fork of `hurryos/iptvs-app` with auto-update removed.

## 修复内容
- 移除 `check_for_updates()` 自动更新逻辑
- 原版会自动从 `iptvs.pes.im` 下载"更新"，但该接口返回的是 JSON 数据源而非 Python 代码，导致 `app.py` 被覆写后无法运行，容器无限重启

## 使用
```bash
docker run -d --name iptv-app --restart=unless-stopped \
  -p 8453:5000 \
  -v /opt/iptv-data:/app/data \
  mrz2333/iptvs-app-fixed:latest
```

- 访问 `/iptv` 获取 m3u8
- 访问 `/txt` 获取 txt
- 访问 `/` 查看状态

## 原始项目
https://hub.docker.com/r/hurryos/iptvs-app
