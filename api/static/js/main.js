/**
 * Noah System - Main UI Logic
 * Includes: Sidebar management, Service toggles, Toasts, and API Interceptors.
 */

(function() {
  'use strict';

  // Constants
  const MOBILE_BREAKPOINT = 1024;
  
  document.addEventListener('DOMContentLoaded', function() {
    console.log('[System] Dashboard UI Initializing...');
    
    // ── Sidebar Management ────────────────────────────────────
    const sidebarToggle = document.getElementById('sidebar-toggle');
    const toggleIcon    = document.getElementById('toggle-icon');
    
    function setSidebarState(isCollapsed) {
      if (isCollapsed) {
        document.body.classList.add('sidebar-collapsed');
        if (toggleIcon) toggleIcon.setAttribute('data-lucide', 'chevron-right');
      } else {
        document.body.classList.remove('sidebar-collapsed');
        if (toggleIcon) toggleIcon.setAttribute('data-lucide', 'chevron-left');
      }
      localStorage.setItem('sidebar_collapsed', isCollapsed);
      if (window.lucide) lucide.createIcons();
    }

    // Initialize from storage
    const savedState = localStorage.getItem('sidebar_collapsed') === 'true';
    setSidebarState(savedState);

    if (sidebarToggle) {
      sidebarToggle.addEventListener('click', () => {
        const currentlyCollapsed = document.body.classList.contains('sidebar-collapsed');
        setSidebarState(!currentlyCollapsed);
      });
    }

    // ── Active Link Highlighting ──────────────────────────────
    function markActiveLinks() {
      const currentPath = window.location.pathname;
      document.querySelectorAll('.nav-item').forEach(link => {
        const linkPath = link.getAttribute('href');
        if (linkPath === currentPath) {
          link.classList.add('active');
        } else {
          link.classList.remove('active');
        }
      });
    }
    markActiveLinks();

    // ── API Action Interceptor (POST Links) ───────────────────
    // Some links in the sidebar are actually POST actions (e.g., Ingestion)
    document.addEventListener('click', function(e) {
      const link = e.target.closest('a[data-method="POST"]');
      if (link) {
        e.preventDefault();
        const url = link.getAttribute('href');
        handleAction(url, link.title || 'Processing...');
      }
    });

    function handleAction(url, label) {
      showToast('Đang thực hiện: ' + label, 'info');
      fetch(url, { method: 'POST' })
        .then(res => res.json())
        .then(data => {
          if (data.status === 'success') {
            showToast(data.message, 'success');
          } else {
            showToast(data.message, 'error');
          }
        })
        .catch(err => {
          showToast('Lỗi kết nối: ' + err, 'error');
        });
    }

    // ── Toast Service ────────────────────────────────────────
    window.showToast = function(message, type = 'info') {
      const container = document.getElementById('toast-container');
      if (!container) return;

      const toast = document.createElement('div');
      toast.className = `toast-item flex items-center gap-3 px-5 py-4 rounded-2xl shadow-2xl mb-3 border backdrop-blur-md transition-all`;
      
      let icon = 'info';
      if (type === 'success') {
        toast.classList.add('bg-emerald-900/90', 'border-emerald-500/30', 'text-emerald-50');
        icon = 'check-circle';
      } else if (type === 'error') {
        toast.classList.add('bg-rose-900/90', 'border-rose-500/30', 'text-rose-50');
        icon = 'alert-circle';
      } else {
        toast.classList.add('bg-slate-900/90', 'border-slate-700', 'text-slate-100');
        icon = 'info';
      }

      toast.innerHTML = `
        <i data-lucide="${icon}" class="w-5 h-5"></i>
        <span class="text-sm font-bold">${message}</span>
      `;

      container.appendChild(toast);
      if (window.lucide) lucide.createIcons();

      // Auto remove
      setTimeout(() => {
        toast.style.animation = 'toast-out 0.4s ease forwards';
        setTimeout(() => toast.remove(), 400);
      }, 4000);
    };

    // ── Service Control Logic ────────────────────────────────
    window.toggleService = function(service, action) {
      showToast(`Đang gửi lệnh ${action} tới ${service}...`, 'info');
      
      fetch('/api/ops/service-toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ service: service, action: action })
      })
      .then(res => res.json())
      .then(data => {
        if (data.status === 'success') {
          showToast(data.message || 'Thành công!', 'success');
          setTimeout(updateServiceStatus, 2000);
        } else {
          // Show real error from Docker CLI (if any)
          showToast(data.message || 'Lỗi không xác định', 'error');
        }
      })
      .catch(err => {
        showToast('Lỗi kết nối: ' + err, 'error');
      });
    };

    function updateServiceStatus() {
      fetch('/api/ops/service-status')
        .then(res => res.json())
        .then(res => {
          if (res.status === 'success' && res.data) {
            ['worker', 'producer', 'legacy'].forEach(svc => {
              const badge = document.getElementById(svc + '-status');
              if (badge) {
                const isRunning = res.data[svc];
                badge.className = `w-2 h-2 rounded-full transition-all duration-500 ${
                  isRunning ? 'bg-emerald-500 shadow-[0_0_12px_rgba(16,185,129,0.6)] animate-pulse' : 'bg-slate-500 shadow-none'
                }`;
              }
            });
          }
        })
        .catch(e => console.warn('Status poll failed:', e));
    }

    // Start status polling
    setInterval(updateServiceStatus, 5000);
    updateServiceStatus();

    // ── Profile Dropdown Toggle ──────────────────────────────
    const profileTrigger = document.getElementById('profile-trigger');
    const profileMenu    = document.getElementById('profile-menu');
    if (profileTrigger && profileMenu) {
      profileTrigger.addEventListener('click', (e) => {
        e.stopPropagation();
        const isVisible = !profileMenu.classList.contains('invisible');
        profileMenu.classList.toggle('opacity-0', isVisible);
        profileMenu.classList.toggle('invisible', isVisible);
        profileMenu.classList.toggle('scale-95', isVisible);
      });
      document.addEventListener('click', () => {
        if (profileMenu) profileMenu.classList.add('opacity-0', 'invisible', 'scale-95');
      });
    }

  });
})();
