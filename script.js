/**
 * RAIKU Leaderboard — Main Script
 * Loads users.json, computes rankings, renders podium + table, search & filter.
 */

(function () {
    'use strict';

    /* ── DOM refs ── */
    const $body = document.getElementById('leaderboardBody');
    const $podium = document.getElementById('podium');
    const $pagination = document.getElementById('pagination');
    const $searchInput = document.getElementById('searchInput');
    const $searchClear = document.getElementById('searchClear');
    const $channelFilter = document.getElementById('channelFilter');
    const $perPageFilter = document.getElementById('perPageFilter');
    const $totalUsers = document.getElementById('totalUsers');
    const $totalMessages = document.getElementById('totalMessages');
    const $emptyState = document.getElementById('emptyState');
    const $tableWrapper = document.querySelector('.table-wrapper');

    /* ── State ── */
    let allUsers = [];          // parsed + enriched
    let filteredUsers = [];     // after search & channel filter
    let currentPage = 1;
    let perPage = 50;
    let searchQuery = '';
    let channelFilterValue = 'all';
    let allChannels = new Set();

    // Excluded non-language channels
    const UTILITY_CHANNELS = [
        '‼️︱notifications',
        '✅︱verify',
        '🆘︱support-ticket',
        '🎨︱community-content',
        '👋︱welcome',
        '📜︱raiku-tweets',
        '🔗︱official-links',
        '🗺️︱choose-language'
    ];

    // Ignored accounts (by тегнейм)
    const IGNORED_TAGS = ['PolyFeed#8171'];

    /* ── Helpers ── */
    const fmt = (n) => n.toLocaleString('en-US');

    function totalMessages(user) {
        const channels = user['количество_сообщений_в_разных_каналах'];
        if (!channels) return 0;
        return Object.values(channels).reduce((a, b) => a + b, 0);
    }

    function channelCount(user, channel) {
        const channels = user['количество_сообщений_в_разных_каналах'];
        if (!channels || !channels[channel]) return 0;
        return channels[channel];
    }

    function displayName(user) {
        return user['ник_на_сервере'] || user['тегнейм'] || 'Unknown';
    }

    function cleanChannelName(ch) {
        // Remove emoji prefixes like 🏠︱ or 🇷🇺︱
        return ch.replace(/^[^\w]*[︱|]+/u, '').replace(/^[\s]+/, '') || ch;
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function highlightMatch(text, query) {
        if (!query) return escapeHtml(text);
        const escaped = escapeHtml(text);
        const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
        return escaped.replace(regex, '<span class="highlight">$1</span>');
    }

    function defaultAvatar() {
        return 'data:image/svg+xml,' + encodeURIComponent(
            '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128"><rect fill="#111318" width="128" height="128"/><circle cx="64" cy="48" r="22" fill="#2a2d35"/><ellipse cx="64" cy="100" rx="34" ry="24" fill="#2a2d35"/></svg>'
        );
    }

    /* ── Data loading ── */
    async function loadData() {
        try {
            const resp = await fetch('users.json');
            const raw = await resp.json();

            // Filter out ignored accounts, enrich with total + sort
            allUsers = raw
                .filter(u => !IGNORED_TAGS.includes(u['тегнейм']))
                .map(u => ({
                    ...u,
                    _total: totalMessages(u),
                    _name: displayName(u),
                }));

            // Sort by total desc
            allUsers.sort((a, b) => b._total - a._total);

            // Collect all channels
            allUsers.forEach(u => {
                const ch = u['количество_сообщений_в_разных_каналах'];
                if (ch) Object.keys(ch).forEach(c => allChannels.add(c));
            });

            populateChannelFilter();
            applyFilters();
            updateStats();
        } catch (err) {
            console.error('Failed to load users.json:', err);
            $body.innerHTML = `<tr><td colspan="4" style="text-align:center;padding:40px;color:var(--text-secondary)">Failed to load data. Make sure users.json is in the same directory.</td></tr>`;
        }
    }

    /* ── Stats ── */
    function updateStats() {
        $totalUsers.textContent = fmt(allUsers.length);
        const total = allUsers.reduce((sum, u) => sum + u._total, 0);
        $totalMessages.textContent = fmt(total);
    }

    /* ── Channel filter population (language channels only) ── */
    function populateChannelFilter() {
        const sorted = [...allChannels]
            .filter(ch => !UTILITY_CHANNELS.includes(ch))
            .sort();

        sorted.forEach(ch => {
            const opt = document.createElement('option');
            opt.value = ch;
            opt.textContent = ch;
            $channelFilter.appendChild(opt);
        });
    }

    /* ── Filter + Search ── */
    function applyFilters() {
        let result = allUsers;

        // Channel filter
        if (channelFilterValue !== 'all') {
            result = result.filter(u => {
                const ch = u['количество_сообщений_в_разных_каналах'];
                return ch && ch[channelFilterValue] && ch[channelFilterValue] > 0;
            });
            // Re-sort by that channel
            result.sort((a, b) => channelCount(b, channelFilterValue) - channelCount(a, channelFilterValue));
        }

        // Search (by tag only)
        if (searchQuery) {
            const q = searchQuery.toLowerCase();
            result = result.filter(u => {
                const tag = (u['тегнейм'] || '').toLowerCase();
                return tag.includes(q);
            });
        }

        filteredUsers = result;
        currentPage = 1;
        render();
    }

    /* ── Rendering ── */
    function render() {
        const totalPages = Math.ceil(filteredUsers.length / perPage) || 1;
        if (currentPage > totalPages) currentPage = totalPages;

        const showPodium = !searchQuery && currentPage === 1;
        const start = (currentPage - 1) * perPage;
        const pageUsers = filteredUsers.slice(start, start + perPage);

        // Empty state
        if (filteredUsers.length === 0) {
            $emptyState.style.display = 'block';
            $tableWrapper.style.display = 'none';
            $podium.style.display = 'none';
            $pagination.innerHTML = '';
            return;
        } else {
            $emptyState.style.display = 'none';
            $tableWrapper.style.display = '';
        }

        // Podium
        if (showPodium && filteredUsers.length >= 3) {
            $podium.style.display = 'grid';
            renderPodium(filteredUsers.slice(0, 3));
        } else {
            $podium.style.display = 'none';
        }

        // Table
        renderTable(pageUsers, start, showPodium);

        // Pagination
        renderPagination(totalPages);
    }

    function renderPodium(top3) {
        const medals = ['👑', '', ''];
        const positions = [1, 2, 3];

        $podium.innerHTML = top3.map((user, i) => {
            const pos = positions[i];
            const avatar = user.pfp || defaultAvatar();
            const name = escapeHtml(user._name);
            const tag = escapeHtml(user['тегнейм'] || '');
            const msgs = channelFilterValue !== 'all'
                ? channelCount(user, channelFilterValue)
                : user._total;

            return `
                <div class="podium__card podium__card--${pos} animate-in" style="animation-delay: ${i * 0.1}s">
                    <span class="podium__rank-badge">#${pos}</span>
                    <div class="podium__avatar-wrap">
                        ${pos === 1 ? `<span class="podium__crown">${medals[0]}</span>` : ''}
                        <img class="podium__avatar" src="${avatar}" alt="${name}" loading="lazy" onerror="this.src='${defaultAvatar()}'">
                    </div>
                    <span class="podium__name" title="${name}">${name}</span>
                    <span class="podium__tag">@${tag}</span>
                    <span class="podium__messages">${fmt(msgs)}</span>
                    <span class="podium__messages-label">Messages</span>
                </div>
            `;
        }).join('');
    }

    function renderTable(users, startIndex, hidePodium) {
        const skipFirst = hidePodium ? 3 : 0;

        $body.innerHTML = users.map((user, i) => {
            const globalRank = startIndex + i + 1;
            if (hidePodium && globalRank <= 3) return '';

            const avatar = user.pfp || defaultAvatar();
            const name = escapeHtml(user._name);
            const tag = highlightMatch(user['тегнейм'] || '', searchQuery);
            const msgs = channelFilterValue !== 'all'
                ? channelCount(user, channelFilterValue)
                : user._total;

            // Channel pills (top 3 language channels by count)
            const channels = user['количество_сообщений_в_разных_каналах'];
            let channelHtml = '';
            if (channels) {
                const sorted = Object.entries(channels)
                    .filter(([ch]) => !UTILITY_CHANNELS.includes(ch))
                    .sort((a, b) => b[1] - a[1])
                    .slice(0, 3);
                channelHtml = sorted.map(([ch, count]) =>
                    `<span class="channel-pill">${escapeHtml(ch)}<span class="channel-pill__count">${fmt(count)}</span></span>`
                ).join('');
            }

            // Rank display
            let rankHtml = `${globalRank}`;
            if (globalRank <= 5) {
                rankHtml = `<span class="rank-top rank-top--${globalRank}">${globalRank}</span>`;
            }

            return `
                <tr class="table__tr animate-in" style="animation-delay: ${Math.min(i * 0.02, 0.6)}s">
                    <td class="table__td table__td--rank">${rankHtml}</td>
                    <td class="table__td">
                        <div class="table__user">
                            <img class="table__avatar" src="${avatar}" alt="" loading="lazy" onerror="this.src='${defaultAvatar()}'">
                            <div class="table__user-info">
                                <span class="table__username" title="${escapeHtml(user._name)}">${name}</span>
                                <span class="table__usertag">@${tag}</span>
                            </div>
                        </div>
                    </td>
                    <td class="table__td table__td--messages">${fmt(msgs)}</td>
                    <td class="table__td table__td--channels">
                        <div class="channel-pills">${channelHtml}</div>
                    </td>
                </tr>
            `;
        }).join('');
    }

    function renderPagination(totalPages) {
        if (totalPages <= 1) {
            $pagination.innerHTML = '';
            return;
        }

        const maxVisible = 7;
        let pages = [];

        if (totalPages <= maxVisible) {
            for (let i = 1; i <= totalPages; i++) pages.push(i);
        } else {
            pages.push(1);
            if (currentPage > 3) pages.push('...');

            const start = Math.max(2, currentPage - 1);
            const end = Math.min(totalPages - 1, currentPage + 1);
            for (let i = start; i <= end; i++) pages.push(i);

            if (currentPage < totalPages - 2) pages.push('...');
            pages.push(totalPages);
        }

        let html = `<button class="pagination__btn" ${currentPage === 1 ? 'disabled' : ''} data-page="${currentPage - 1}">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m15 18-6-6 6-6"/></svg>
        </button>`;

        pages.forEach(p => {
            if (p === '...') {
                html += `<span class="pagination__ellipsis">...</span>`;
            } else {
                html += `<button class="pagination__btn ${p === currentPage ? 'pagination__btn--active' : ''}" data-page="${p}">${p}</button>`;
            }
        });

        html += `<button class="pagination__btn" ${currentPage === totalPages ? 'disabled' : ''} data-page="${currentPage + 1}">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m9 18 6-6-6-6"/></svg>
        </button>`;

        $pagination.innerHTML = html;
    }

    /* ── Event Listeners ── */
    let searchTimeout;
    $searchInput.addEventListener('input', () => {
        clearTimeout(searchTimeout);
        searchQuery = $searchInput.value.trim();
        $searchClear.classList.toggle('visible', !!searchQuery);
        searchTimeout = setTimeout(() => applyFilters(), 200);
    });

    $searchClear.addEventListener('click', () => {
        $searchInput.value = '';
        searchQuery = '';
        $searchClear.classList.remove('visible');
        applyFilters();
        $searchInput.focus();
    });

    $channelFilter.addEventListener('change', () => {
        channelFilterValue = $channelFilter.value;
        applyFilters();
    });

    $perPageFilter.addEventListener('change', () => {
        perPage = parseInt($perPageFilter.value, 10);
        currentPage = 1;
        render();
    });

    $pagination.addEventListener('click', (e) => {
        const btn = e.target.closest('.pagination__btn');
        if (!btn || btn.disabled) return;
        const page = parseInt(btn.dataset.page, 10);
        if (isNaN(page)) return;
        currentPage = page;
        render();
        // Scroll to table
        $tableWrapper.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });

    // Keyboard shortcut: Ctrl/Cmd + K → focus search
    document.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
            e.preventDefault();
            $searchInput.focus();
            $searchInput.select();
        }
    });

    /* ── Init ── */
    loadData();
})();
